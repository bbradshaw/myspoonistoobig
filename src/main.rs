use std::cmp::min;
use redis;
use redis::RedisResult;
use humantime::format_duration;
use std::time::Duration;
use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use std::thread;
use std::env;
use std::thread::JoinHandle;
use crossbeam_channel::{bounded, Receiver};
use std::fs;
use std::io::Write;

const NUM_SAMPLES: usize = 1000;
const NUM_WORKERS: usize = 20;

struct KeyStats {
    name: String,
    size: u32,
    idletime: u32,
}

#[derive(Debug, Clone)]
struct BucketStats {
    name: String,
    size: u64,
    count: usize,
    min_idletime: u32,
    avg_idletime: u32,
}

enum WorkMsg {
    FETCH,
    QUIT,
}

enum FlushMsg {
    FLUSH(HashMap<String, BucketStats>),
    QUIT
}

impl KeyStats {
    fn get_stats(con: &mut redis::Connection, name: &str) -> RedisResult<Self> {
        let size = redis::cmd("MEMORY").arg("USAGE").arg(name).query(con)?;
        let idletime = redis::cmd("OBJECT").arg("IDLETIME").arg(name).query(con)?;
        Ok(
            KeyStats {
                name: name.to_owned(),
                size,
                idletime,
            })
    }
}


fn main() {
    let args: Vec<String> = env::args().collect();
    let url = args.get(1).unwrap().as_str();

    let buckets = gather_data(url);
    println!("\n\nDONE!!\n");
    write_to_file(&mut std::io::stdout(), &buckets).unwrap();
}

fn gather_data(redis_url: &str) -> HashMap<String, BucketStats> {
    let mut buckets: HashMap<String, BucketStats> = HashMap::new();

    let client = Arc::new(redis::Client::open(redis_url).unwrap());
    let (workq, rcv) = bounded(NUM_WORKERS + NUM_SAMPLES);
    let (pipeout, finished) = bounded(NUM_WORKERS);
    let mut handles = Vec::with_capacity(NUM_WORKERS);
    for _tn in 0..NUM_WORKERS {
        let client = client.clone();
        let q = rcv.clone();
        let pipeout = pipeout.clone();
        handles.push(thread::spawn(move || {
            let mut con = client.get_connection().unwrap();

            loop {
                let msg = q.recv().unwrap();
                match msg {
                    WorkMsg::FETCH => {
                        let key = get_random_key(&mut con);
                        pipeout.send(key).unwrap();
                    }
                    WorkMsg::QUIT => break
                }
            }
        }));
    }
    let (flush_send, flush_rcv) = bounded(0);
    handles.push(flush_work_thread(flush_rcv));
    for _ in 0..NUM_SAMPLES {
        workq.send(WorkMsg::FETCH).unwrap();
    }
    for _ in 0..NUM_WORKERS {
        workq.send(WorkMsg::QUIT).unwrap();
    }

    for _ in 0..NUM_SAMPLES {
        let msg = finished.recv().unwrap().unwrap();
        add_to_bucket(&mut buckets, msg);
        let _ = flush_send.try_send(FlushMsg::FLUSH(buckets.clone()));
    }

    flush_send.send(FlushMsg::QUIT).unwrap();
    for handle in handles {
        handle.join().unwrap();
    }
    buckets
}

fn get_random_key(con: &mut redis::Connection) -> RedisResult<KeyStats> {
    let name: String = redis::cmd("RANDOMKEY").query(con)?;
    KeyStats::get_stats(con, &name)
}

fn get_bucket(keyname: &str) -> String {
    keyname
        .split(&[':', '_'])
        .filter(|&component| !component.chars().any(char::is_numeric))
        .collect::<Vec<&str>>()
        .join(":")
}

fn add_to_bucket(buckets: &mut HashMap<String, BucketStats>, key_stats: KeyStats) {
    let entry = buckets.entry(get_bucket(&key_stats.name));
    match entry {
        Entry::Occupied(mut occupied) => {
            let bucket_stats = occupied.get_mut();
            bucket_stats.count += 1;
            bucket_stats.size += key_stats.size as u64;
            bucket_stats.avg_idletime += key_stats.idletime.saturating_sub(bucket_stats.avg_idletime) / bucket_stats.count as u32;
            bucket_stats.min_idletime = min(key_stats.idletime, bucket_stats.min_idletime);
        }
        Entry::Vacant(vacant) => {
            let name = vacant.key().to_owned();
            vacant.insert(BucketStats {
                name,
                size: key_stats.size as u64,
                count: 1,
                avg_idletime: key_stats.idletime,
                min_idletime: key_stats.idletime,
            });
        }
    }
}

fn write_to_file(f: &mut impl Write, buckets: &HashMap<String, BucketStats>) -> Result<(), std::io::Error> {
    for bucket in buckets.values() {
        writeln!(f, "\nBucket: {}", &bucket.name)?;
        writeln!(f, "Total Size: {}", bucket.size)?;
        writeln!(f, "Count: {}", bucket.count)?;
        writeln!(f, "Min Idle: {}", format_duration(Duration::new(bucket.min_idletime as u64, 0)))?;
        writeln!(f, "Avg Idle: {}", format_duration(Duration::new(bucket.avg_idletime as u64, 0)))?;
    }
    Ok(())
}

fn flush_work_thread(flushq: Receiver<FlushMsg>) -> JoinHandle<()>{
    thread::spawn( move|| {
        if let Ok(mut state_file) = fs::File::create("state_file.txt"){
            loop {
                let msg = flushq.recv().unwrap();
                match msg {
                    FlushMsg::QUIT => break,
                    FlushMsg::FLUSH(buckets) => {
                        println!("Flushing state with {} keys counted", buckets.values().fold(0usize, |acc, b| acc + b.count));
                        write_to_file(&mut state_file, &buckets).expect("Error writing to state file!");
                    }
                }
            }
        }
        else{
            println!("could not write to state file!!");
        }
    })
}