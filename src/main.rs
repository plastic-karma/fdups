use md5::{Digest, Md5};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::env;
use tokio::task::{self, JoinHandle};
use tokio::{fs, io::AsyncReadExt};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let path = if args.len() > 1 { &args[1] } else { "." };
    let mut file_map: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for x in visit_dirs(Arc::new(path.to_string())).await {
        let (path, hash) = x.await.unwrap();
        if file_map.contains_key(hash.as_str()) {
            let paths = file_map.get_mut(hash.as_str()).unwrap();
            paths.push(path);
        } else {
            let new_hash = hash.clone().to_owned();
            file_map.insert(new_hash, vec![path]);
        }
    }

    let mut counter = 0;
    for (_, paths) in &file_map {
        counter += paths.len();
        if paths.len() > 1 {
            println!("");
            for path in paths {
                println!("{}", path.display());
            }
            println!("");
        }
    }
    println!("Total files: {}", counter);
}

fn visit_dirs(
    dir: Arc<String>,
) -> Pin<Box<dyn Future<Output = Vec<JoinHandle<(std::path::PathBuf, String)>>>>> {
    Box::pin(async move {
        let mut tasks = vec![];
        if Path::new(dir.as_str()).is_dir() {
            let mut read_dir = fs::read_dir(dir.as_str()).await.unwrap();
            while let Some(entry) = read_dir.next_entry().await.unwrap() {
                let path = entry.path();
                if path.is_dir() {
                    let new_path = path.to_str().unwrap().to_string();
                    let result = visit_dirs(Arc::new(new_path)).await;
                    tasks.extend(result);
                } else {
                    if get_file_size(path.as_path()).await.unwrap() < 1024 {
                        continue;
                    }
                    tasks.push(task::spawn({
                        async move {
                            let path = fs::canonicalize(path).await.unwrap();
                            let hash = compute_md5(&path).await.unwrap();
                            return (path, hash);
                        }
                    }));
                }
            }
        }
        return tasks;
    })
}

async fn compute_md5(path: &Path) -> std::io::Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Md5::new();
    let mut buffer = [0; 1024];
    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

async fn get_file_size(path: &Path) -> std::io::Result<u64> {
    let metadata = fs::metadata(path).await?;
    Ok(metadata.len())
}
