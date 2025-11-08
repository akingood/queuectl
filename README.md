# QueueCTL - Background Job Queue System

## 📘 Overview

**QueueCTL** is a CLI-based background job queue system built in **Python**, designed for concurrent job execution, retry logic with exponential backoff, and persistent state management using SQLite.

This project was developed as part of a backend developer internship assignment.

---

## 🚀 Features

- Enqueue background jobs with shell commands
- Multiple concurrent workers
- Persistent storage with SQLite
- Retry mechanism using exponential backoff
- Dead Letter Queue (DLQ) for permanently failed jobs
- Configurable retry count and backoff base
- Graceful worker shutdown
- Job logs stored individually for debugging
- Clean and simple CLI interface

---

## ⚙️ Tech Stack

- **Language:** Python 3.10+  
- **Database:** SQLite  
- **CLI:** argparse  
- **Concurrency:** threading  

---

## 📂 Project Structure

```
.
|-- queuectl.py
|-- queuectl.db
|-- queuectl_config.json
|-- job_logs/
|   |-- <job_id>.log
|-- README.md
|-- .gitignore
|-- venv/
|-- run_task.sh
```

---

## 💻 Setup Instructions

1. **Clone Repository**
   ```bash
   git clone <your_repo_url>
   cd queuectl
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # on Windows
   ```

3. **Install Dependencies**
   *(No external packages required — pure Python stdlib)*

4. **Run QueueCTL Commands**
   ```bash
   python queuectl.py --help
   ```

---

## 🧩 CLI Usage Examples

### Enqueue Job
```bash
python queuectl.py enqueue "echo 'Hello QueueCTL'"
```

### Start Workers
```bash
python queuectl.py worker start --count 2
```

### View Status
```bash
python queuectl.py status
```

### List Jobs
```bash
python queuectl.py list --state completed
```

### Retry DLQ Job
```bash
python queuectl.py dlq retry <job_id>
```

---

## 🔄 Job Lifecycle

| **State** | **Description** |
|------------|-----------------|
| `pending` | Waiting for a worker |
| `processing` | Currently being executed |
| `completed` | Executed successfully |
| `failed` | Retryable failure |
| `dead` | Permanently failed, moved to DLQ |

---

## 🧠 Execution Flow

1. **Enqueue** → Insert job in SQLite (`state=pending`)
2. **Worker Start** → Picks a pending job and executes it
3. **Processing** → State updated to `processing`
4. **Completion**
   - If exit code `0`: mark as `completed`
   - If failure: retry with exponential backoff
5. **Retry** → If retries exceed limit → move to DLQ (`state=dead`)

---

## ⚙️ Configuration

Config is stored in `queuectl_config.json`:
```json
{
  "max_retries": 3,
  "backoff_base": 2
}
```

To modify via CLI:
```bash
python queuectl.py config set max-retries 5
```

---

## 📊 Example Status Output

```
Workers (registered): 12
pending: 1 | processing: 0 | completed: 4 | dlq: 3
```

### Explanation:
- **Workers (registered):** Total workers ever started
- **pending:** Jobs waiting for processing
- **processing:** Jobs currently being executed
- **completed:** Successfully finished jobs
- **dlq:** Failed jobs moved to Dead Letter Queue

---

## 🧪 Testing Scenarios

✅ Successful job execution  
✅ Failed job retries with exponential delay  
✅ DLQ population after retries exhausted  
✅ Job persistence after restart  
✅ Multiple workers without duplicate execution  

---

## 🏗️ Architecture Overview

- **CLI Layer:** `argparse` for subcommands (`enqueue`, `worker`, `list`, etc.)
- **Persistence Layer:** SQLite for storing job states and worker info
- **Worker System:** Thread-based concurrent execution
- **Retry Logic:** Exponential backoff with configurable retries
- **DLQ Management:** Separate table for dead jobs, retryable via CLI
- **Logging:** Individual job logs stored under `/job_logs`

---

## 🧾 Submission Info

Developed by **Akilesh V**  
Email: [akivenky10@gmail.com](mailto:akivenky10@gmail.com)  

---

## ✅ Checklist

- [✅] Enqueue jobs  
- [✅] Worker management  
- [✅] Retry + Backoff  
- [✅] DLQ implemented  
- [✅] Persistent DB  
- [✅] Configurable parameters  
- [✅] Graceful shutdown  
- [✅] Readable and modular code  

---

> © 2025 Akilesh V — QueueCTL CLI System
