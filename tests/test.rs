
#[cfg(test)]
mod tests {
    use storage::Storage;

    use super::*;

    #[tokio::test]
    async fn test_storage_operations() {
        Storage::init(
            "sftp.example.com",
            22,
            "username",
            "password",
            Some("/base/dir")
        ).await.unwrap();

        // Ejemplo de escritura
        Storage::write("test.txt", "Me vengo en byron").await.unwrap();
        
        // Ejemplo de lectura
        let content = Storage::get_file("test.txt").await.unwrap();
        println!("Content: {}", content);
        
    }
}