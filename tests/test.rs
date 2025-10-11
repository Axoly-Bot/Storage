
#[cfg(test)]
mod tests {
    use storage::Storage;


    #[tokio::test]
    async fn test_storage_operations() {
        Storage::init(
            "sftp.example.com",
            22,
            "username",
            "password",
            Some("/base/dir")
        )
        .await.unwrap();

        
        Storage::append_line("test.txt", "skibidi toilet").await.unwrap();
    

        // Ejemplo de lectura
        let content = Storage::get_file("test.txt").await.unwrap();
        println!("Content: {}", content);
        
    }
}