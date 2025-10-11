use std::path::{Path, PathBuf};
use std::sync::Arc;
use once_cell::sync::OnceCell;
use anyhow::{Result, Context, anyhow};
use russh_sftp::protocol::OpenFlags;
use serde::{Serialize, Deserialize};
use tokio::task;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use russh::{client};
use russh_sftp::{ protocol::{FileAttributes, FileType}};
use russh_sftp::client::SftpSession;

// Instancia estática
static INSTANCE: OnceCell<Storage> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct Storage {
    host: String,
    port: u16,
    username: String,
    password: String,
    base_path: PathBuf,
}

struct ClientHandler;

impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(&mut self, _server_public_key: &russh::keys::PublicKey) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

impl Storage {
    // =================================================================
    // Setup y Wrappers
    // =================================================================

    /// Inicializa el almacenamiento con SFTP (async)
    pub async fn init(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        base_path: Option<&str>,
    ) -> Result<()> {
        let base_path = match base_path {
            Some(path) => PathBuf::from(path),
            None => PathBuf::from("/"),
        };

        let storage = Storage {
            host: host.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            base_path,
        };
        
        storage.test_connection().await?;
        
        INSTANCE.set(storage)
            .map_err(|_| anyhow!("Storage already initialized"))?;
        
        Ok(())
    }

    /// Obtiene la instancia global de Storage
    fn get_global() -> &'static Storage {
        INSTANCE.get().expect("Storage is not initialized. Call Storage::init() first.")
    }

    pub async fn check_connection() -> Result<()> {
        let storage = Self::get_global();
        storage.test_connection().await
    }

    /// Construye la ruta completa dentro del base_path
    fn build_path(&self, path: &str) -> PathBuf {
        let relative_path = path.trim_start_matches('/');
        self.base_path.join(Path::new(relative_path))
    }

    /// Test de conexión
    async fn test_connection(&self) -> Result<()> {
        self.with_sftp(|_| async { Ok(()) }).await
    }

    /// Configuración del cliente SSH
    fn create_ssh_config() -> client::Config {
        client::Config::default()
    }

    /// Ejecuta una operación con SFTP (usando russh-sftp)
    async fn with_sftp<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(SftpSession) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let host = self.host.clone();
        let port = self.port;
        let username = self.username.clone();
        let password = self.password.clone();

        task::spawn(async move {
            // Crear y configurar el cliente SSH
            let config = Arc::new(Self::create_ssh_config());
            let handler = ClientHandler;
            
            let mut client = client::connect(
                config, 
                format!("{}:{}", host, port), 
                handler
            ).await
            .context("Failed to connect to SSH server")?;

            // Autenticación
            client
                .authenticate_password(&username, &password)
                .await
                .context("SSH password authentication failed")?;

            // Abrir canal para SFTP
            let channel = client
                .channel_open_session()
                .await
                .context("Failed to open SSH channel")?;

            // Crear sesión SFTP
            let sftp = SftpSession::new(channel.into_stream())
                .await
                .context("Failed to create SFTP session")?;

            // Ejecutar la operación
            operation(sftp).await
        }).await?
    }

    /// Ejecuta una operación con un archivo SFTP
    async fn with_file<F, Fut, T>(&self, path: &str, operation: F) -> Result<T>
    where
        F: FnOnce(russh_sftp::client::fs::File) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let full_path = self.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        self.with_sftp(move |sftp| async move {
            let file = sftp
                .open(&path_string)
                .await
                .context(format!("Failed to open file: {}", path_string))?;
            
            operation(file).await
        }).await
    }

    /// Ejecuta una operación con un archivo SFTP para escritura
    async fn with_file_write<F, Fut, T>(&self, path: &str, operation: F) -> Result<T>
    where
        F: FnOnce(russh_sftp::client::fs::File) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let full_path = self.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        self.with_sftp(move |sftp| async move {
            let file = sftp
                .create(&path_string)
                .await
                .context(format!("Failed to create file for writing: {}", path_string))?;
            
            operation(file).await
        }).await
    }

    /// Ejecuta una operación con un archivo SFTP para append
    async fn with_file_append<F, Fut, T>(&self, path: &str, operation: F) -> Result<T>
    where
        F: FnOnce(russh_sftp::client::fs::File) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let full_path = self.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        self.with_sftp(move |sftp| async move {
            // Intentar abrir en modo append (sin CREATE)
            let file = match sftp.open_with_flags(&path_string, OpenFlags::WRITE | OpenFlags::APPEND).await {
                Ok(file) => file,
                Err(e) => {
                    // Si el error es porque el archivo no existe, crearlo y luego abrirlo en modo append.
                    if e.to_string().contains("No such file") {
                        // Crear el archivo (vacío)
                        let _ = sftp.create(&path_string).await?;
                        // Ahora abrirlo en modo append
                        sftp.open_with_flags(&path_string, OpenFlags::WRITE | OpenFlags::APPEND).await?
                    } else {
                        return Err(e).context("Failed to open file for append");
                    }
                }
            };
            
            operation(file).await
        }).await
    }

    // =================================================================
    // OPERACIONES CRUD ASYNC
    // =================================================================
    pub async fn batch<F, Fut, T>(operation: F) -> Result<T>
    where
        F: FnOnce(SftpSession) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let storage = Self::get_global();
        storage.with_sftp(operation).await
    }        

    pub async fn append_line(path: &str, content: &str) -> Result<()> {
        let storage = Self::get_global();
        
        // CLONA los strings antes de moverlos al closure
        let path_clone = path.to_string();
        let content_clone = content.to_string();
        
        storage.with_sftp(move |sftp| async move {
            let full_path = storage.build_path(&path_clone);
            let path_string = full_path.to_string_lossy().into_owned();
            
            // Leer contenido existente
            let existing_content = match sftp.open(&path_string).await {
                Ok(mut file) => {
                    let mut content = String::new();
                    let _ = file.read_to_string(&mut content).await;
                    content
                }
                Err(_) => String::new(), // Archivo no existe
            };
            
            // Construir nuevo contenido
            let new_content = if existing_content.is_empty() {
                format!("{}\n", content_clone)
            } else if existing_content.ends_with('\n') {
                format!("{}{}\n", existing_content, content_clone)
            } else {
                format!("{}\n{}\n", existing_content, content_clone)
            };
            
            // Escribir todo el contenido
            let mut file = sftp
                .create(&path_string)
                .await
                .context(format!("Failed to create file: {}", path_string))?;
            
            file.write_all(new_content.as_bytes()).await
                .context("Failed to write file content")?;
            file.flush().await
                .context("Failed to flush file")?;
            
            Ok(())
        }).await
    }
    /// Verifica si un archivo existe (async)
    pub async fn file_exists(path: &str) -> Result<bool> {
        let storage = Self::get_global();
        let full_path = storage.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        storage.with_sftp(move |sftp| async move {
            match sftp.metadata(&path_string).await {
                Ok(_) => Ok(true),
                Err(e) => {
                    if e.to_string().contains("No such file") {
                        Ok(false)
                    } else {
                        Err(anyhow!("Error checking file existence for {}: {}", path_string, e))
                    }
                }
            }
        }).await
    }
    
    /// Obtiene el tamaño de un archivo en bytes (async)
    pub async fn get_file_size(path: &str) -> Result<u64> {
        let storage = Self::get_global();
        let full_path = storage.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        storage.with_sftp(move |sftp| async move {
            let metadata = sftp
                .metadata(&path_string)
                .await
                .context("Failed to get file metadata")?;
            
            Ok(metadata.size.unwrap_or(0))
        }).await
    }

    /// Escribe un archivo de texto (sobrescribe si existe) (async)
    pub async fn write(path: &str, content: &str) -> Result<()> {
        let storage = Self::get_global();
        let content = content.to_string();

        storage.with_file_write(path, move |mut file| async move {
            file.write_all(content.as_bytes()).await
                .context("Failed to write file content")?;
            file.flush().await
                .context("Failed to flush file")?;
            Ok(())
        }).await
    }

    /// Escribe un archivo con contenido en una nueva línea (append) (async)
    pub async fn write_line(path: &str, content: &str) -> Result<()> {
        let storage = Self::get_global();
        let content_with_newline = format!("{}\n", content);

        storage.with_file_append(path, move |mut file| async move {
            file.write_all(content_with_newline.as_bytes()).await
                .context("Failed to append file content")?;
            file.flush().await
                .context("Failed to flush file")?;
            Ok(())
        }).await
    }

    /// Escribe datos binarios (async)
    pub async fn write_bytes(path: &str, data: &[u8]) -> Result<()> {
        let storage = Self::get_global();
        let data = data.to_vec();

        storage.with_file_write(path, move |mut file| async move {
            file.write_all(&data).await
                .context("Failed to write binary data")?;
            file.flush().await
                .context("Failed to flush file")?;
            Ok(())
        }).await
    }

    /// Escribe una estructura serializable como JSON (async)
    pub async fn write_json<T: Serialize>(path: &str, data: &T) -> Result<()> {
        let json_string = serde_json::to_string_pretty(data)?;
        Self::write(path, &json_string).await
    }

    /// Obtiene el contenido binario de un archivo (async)
    pub async fn get_file_bytes(path: &str) -> Result<Vec<u8>> {
        let storage = Self::get_global();

        storage.with_file(path, |mut file| async move {
            let mut content = Vec::new();
            file.read_to_end(&mut content).await
                .context("Failed to read file content")?;
            Ok(content)
        }).await
    }

    /// Obtiene el contenido de un archivo como String (async)
    pub async fn get_file(path: &str) -> Result<String> {
        let bytes = Self::get_file_bytes(path).await?;
        String::from_utf8(bytes).context("Failed to convert file content to UTF-8 string")
    }

    /// Lee una estructura deserializable desde un archivo JSON (async)
    pub async fn read_json<T>(path: &str) -> Result<T> 
    where
        T: for<'de> Deserialize<'de> + Send + 'static,
    {
        let json_string = Self::get_file(path).await?;
        serde_json::from_str(&json_string).context("Failed to deserialize JSON content")
    }

    /// Elimina un archivo (async)
    pub async fn delete_file(path: &str) -> Result<()> {
        let storage = Self::get_global();
        let full_path = storage.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        storage.with_sftp(move |sftp| async move {
            sftp.remove_file(&path_string).await
                .context("Failed to delete file")
        }).await
    }

    /// Mueve o renombra un archivo (async)
    pub async fn move_file(source: &str, destination: &str) -> Result<()> {
        let storage = Self::get_global();
        let full_source = storage.build_path(source);
        let full_dest = storage.build_path(destination);
        let source_string = full_source.to_string_lossy().into_owned();
        let dest_string = full_dest.to_string_lossy().into_owned();

        storage.with_sftp(move |sftp| async move {
            sftp.rename(&source_string, &dest_string).await
                .context("Failed to move/rename file")
        }).await
    }

    /// Crea un directorio (async)
    pub async fn create_dir(path: &str) -> Result<()> {
        let storage = Self::get_global();
        let full_path = storage.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        storage.with_sftp(move |sftp| async move {
            sftp.create_dir(&path_string).await
                .context("Failed to create directory")
        }).await
    }

    /// Elimina un directorio (async)
    pub async fn remove_dir(path: &str) -> Result<()> {
        let storage = Self::get_global();
        let full_path = storage.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        storage.with_sftp(move |sftp| async move {
            sftp.remove_dir(&path_string).await
                .context("Failed to remove directory")
        }).await
    }

    /// Lista los archivos en un directorio (async)
    pub async fn list_files(prefix: Option<&str>) -> Result<Vec<String>> {
        let storage = Self::get_global();
        let list_path = match prefix {
            Some(p) => storage.base_path.join(Path::new(p.trim_start_matches('/'))),
            None => storage.base_path.clone(),
        };
        let path_string = list_path.to_string_lossy().into_owned();

        storage.with_sftp(move |sftp| async move {
            let mut files = Vec::new();
            
            match sftp.read_dir(&path_string).await {
                Ok(entries) => {
                    for entry in entries {
                        // En russh-sftp, las entradas del directorio tienen un campo 'filename'
                        files.push(entry.file_name());
                    }
                    Ok(files)
                }
                Err(e) => {
                    if e.to_string().contains("No such file") {
                        Ok(Vec::new())
                    } else {
                        Err(anyhow!("Error listing directory {}: {}", path_string, e))
                    }
                }
            }
        }).await
    }

    /// Obtiene metadatos de un archivo/directorio
    pub async fn get_metadata(path: &str) -> Result<FileAttributes> {
        let storage = Self::get_global();
        let full_path = storage.build_path(path);
        let path_string = full_path.to_string_lossy().into_owned();

        storage.with_sftp(move |sftp| async move {
            sftp.metadata(&path_string).await
                .context("Failed to get file metadata")
        }).await
    }

    /// Verifica si es un directorio
    pub async fn is_dir(path: &str) -> Result<bool> {
        let metadata = Self::get_metadata(path).await?;
        Ok(metadata.file_type() == FileType::Dir)
    }

    /// Verifica si es un archivo
    pub async fn is_file(path: &str) -> Result<bool> {
        let metadata = Self::get_metadata(path).await?;
        Ok(metadata.file_type() == FileType::File)
    }
}

// =================================================================
// Ejemplo de uso
// =================================================================
