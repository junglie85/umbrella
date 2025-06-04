use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
};

fn main() {
    umbrella::rain();

    let scheduler = TaskScheduler::default();

    let task_b = Task::new(move |scheduler| {
        println!("Task B runs after task A");

        let task_c = Task::new(move |_| {
            println!("Task C");
        });

        scheduler.submit_tasks([task_c]);
    });
    let b_dependency_count = task_b.dependency_counter();

    let task_a = Task::new(move |_| {
        println!("Task A");

        b_dependency_count.decrement();
    });

    scheduler.submit_tasks([task_a, task_b]);
    scheduler.shutdown();
}

pub struct Task {
    cb: Box<dyn FnOnce(&SchedulerProxy) + Send + Sync + 'static>,
    dependency_count: Arc<AtomicUsize>,
}

impl Task {
    pub fn new<F>(cb: F) -> Self
    where
        F: FnOnce(&SchedulerProxy) + Send + Sync + 'static,
    {
        Self {
            cb: Box::new(cb),
            dependency_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn dependency_counter(&self) -> DependencyCounter {
        DependencyCounter::new(&self.dependency_count)
    }

    fn is_ready(&self) -> bool {
        self.dependency_count.load(Ordering::SeqCst) == 0
    }
}

pub struct DependencyCounter {
    dependency_count: Arc<AtomicUsize>,
}

impl DependencyCounter {
    fn new(dependency_count: &Arc<AtomicUsize>) -> Self {
        let counter = Self {
            dependency_count: Arc::clone(dependency_count),
        };
        counter.increment();
        counter
    }

    fn increment(&self) {
        self.dependency_count.fetch_add(1, Ordering::AcqRel);
    }

    pub fn decrement(self) {
        self.dependency_count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Clone for DependencyCounter {
    fn clone(&self) -> Self {
        Self::new(&self.dependency_count)
    }
}

struct WorkerQueue {
    tasks: Mutex<VecDeque<Task>>,
    condvar: Condvar,
}

struct Worker {
    thread: JoinHandle<()>,
    queue: Arc<WorkerQueue>,
    shutdown: Arc<AtomicBool>,
}

impl Worker {
    fn new() -> Self {
        let queue = Arc::new(WorkerQueue {
            tasks: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        });
        let shutdown = Arc::new(AtomicBool::new(false));

        let thread = {
            let queue = Arc::clone(&queue);
            let shutdown = Arc::clone(&shutdown);

            let thread = std::thread::spawn(move || {
                loop {
                    let task = {
                        let mut tasks = queue.tasks.lock().unwrap();
                        while tasks.is_empty() && !shutdown.load(Ordering::SeqCst) {
                            tasks = queue.condvar.wait(tasks).unwrap();
                        }

                        if tasks.is_empty() && shutdown.load(Ordering::SeqCst) {
                            return;
                        }

                        tasks.pop_front()
                    };

                    let proxy = SchedulerProxy { queue: &queue };

                    if let Some(task) = task {
                        if task.is_ready() {
                            (task.cb)(&proxy);
                        } else {
                            proxy.submit_tasks([task]);
                        }
                    }
                }
            });

            thread
        };

        Self {
            thread,
            queue,
            shutdown,
        }
    }

    fn push(&self, task: Task) {
        if !self.shutdown.load(Ordering::SeqCst) {
            self.queue.tasks.lock().unwrap().push_back(task);
            self.queue.condvar.notify_one();
        }
    }

    fn shutdown(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.queue.condvar.notify_all();
        self.thread.join().expect("todo");
    }
}

struct TaskScheduler {
    workers: Vec<Worker>,
    next_worker: AtomicUsize,
}

impl Default for TaskScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskScheduler {
    fn new() -> Self {
        let thread_count = 4;

        let workers = (0..thread_count)
            .into_iter()
            .map(|_| Worker::new())
            .collect();

        Self {
            workers,
            next_worker: AtomicUsize::new(0),
        }
    }

    fn submit_tasks<I>(&self, tasks: I)
    where
        I: IntoIterator<Item = Task>,
    {
        let worker_count = self.workers.len();
        for task in tasks.into_iter() {
            let index = self.next_worker.fetch_add(1, Ordering::AcqRel) % worker_count;
            let worker = &self.workers[index];
            worker.push(task);
        }
    }

    pub fn shutdown(mut self) {
        for worker in self.workers.drain(..) {
            worker.shutdown();
        }
    }
}

pub struct SchedulerProxy<'a> {
    queue: &'a WorkerQueue,
}

impl<'a> SchedulerProxy<'a> {
    pub fn submit_tasks<I>(&self, tasks: I)
    where
        I: IntoIterator<Item = Task>,
    {
        self.queue.tasks.lock().unwrap().extend(tasks);
    }
}
