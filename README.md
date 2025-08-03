# SQLite_clone

# RustDB - A B-Tree Database Implementation

A from-scratch database implementation in Rust, featuring B-tree storage, SQL parsing, and disk persistence. This project translates and extends the popular ["Let's Build a Simple Database"](https://cstack.github.io/db_tutorial/) tutorial from C to Rust.

## 🚀 Features

- **SQL Interface**: Basic INSERT and SELECT operations
- **B-Tree Storage Engine**: Efficient data organization with automatic node splitting
- **Disk Persistence**: Data survives program restarts
- **Multi-level Trees**: Handles datasets larger than memory
- **Binary Search**: Fast key lookups within nodes
- **Memory Safety**: All the benefits of Rust's ownership system

## 📋 What's Implemented

### Core Database Features

- ✅ REPL (Read-Eval-Print Loop) interface
- ✅ SQL compiler and virtual machine
- ✅ In-memory append-only single-table storage
- ✅ Persistence to disk with paging system
- ✅ Cursor abstraction for table traversal

### B-Tree Implementation

- ✅ B-tree leaf node format and operations
- ✅ Binary search within nodes
- ✅ Leaf node splitting when full
- ✅ Internal node management
- ✅ Recursive B-tree searching
- ✅ Multi-level B-tree traversal
- ✅ Parent node updates after splits
- ✅ Internal node splitting

### Additional Features

- ✅ Duplicate key detection
- ✅ Tree visualization (`.btree` command)
- ✅ Debug constants display (`.constants` command)
- ✅ Proper error handling for edge cases

## 🛠️ Usage

### Prerequisites

- Rust 1.70+
- Cargo

### Dependencies

Add to your `Cargo.toml`:

```toml
[dependencies]
scan_fmt = "0.2"
memoffset = "0.9"
```

### Running the Database

```bash
# Clone the repository
git clone <your-repo-url>
cd rustdb

# Build and run
cargo run -- database.db
```

### Basic Operations

```sql
db > insert 1 john john@example.com
Executed successfully.

db > insert 2 jane jane@example.com
Executed successfully.

db > select
(1, john, john@example.com)
(2, jane, jane@example.com)
Executed successfully.

db > .btree
Tree:
- leaf (size 2)
  - 1
  - 2

db > .exit
```

## 🏗️ Architecture

### Storage Layout

- **Page Size**: 4096 bytes (matches OS page size)
- **Node Types**: Leaf nodes (store data) and Internal nodes (store keys + pointers)
- **Row Format**: Fixed-size records (ID: u32, Username: 32 bytes, Email: 255 bytes)

### B-Tree Structure

```
[Root Internal Node]
     /        \
[Leaf Node]  [Leaf Node]
   |            |
[Data Rows]  [Data Rows]
```

### File Format

Each page contains either:

- **Leaf Node**: Header + Cell array (key-value pairs)
- **Internal Node**: Header + Key array + Child pointer array

## 🧩 Key Challenges Solved

### 1. **Rust Borrow Checker vs Tree Operations**

B-trees require frequent mutable access to different nodes simultaneously. Solved by:

- Careful borrowing scope management
- Strategic use of temporary data collection
- Splitting operations into discrete phases

### 2. **Memory Layout Compatibility**

Ensuring consistent byte layouts for disk persistence:

- Using `#[repr(C)]` for structs
- Manual serialization/deserialization
- Little-endian byte ordering

### 3. **Node Splitting Logic**

Complex algorithm requiring coordination between parent and child nodes:

- Handling root splits (creating new root)
- Updating parent pointers after splits
- Maintaining B-tree invariants

## 🚧 Current Limitations

- **Read-Only Operations**: Only INSERT and SELECT (no UPDATE/DELETE)
- **Single Table**: No support for multiple tables
- **No Transactions**: No ACID properties or rollback
- **Limited SQL**: No JOINs, WHERE clauses, or complex queries
- **No Concurrency**: Single-threaded operation only
- **Fixed Schema**: Hard-coded table structure

## 🔮 Potential Extensions

- [ ] UPDATE and DELETE operations
- [ ] WHERE clause filtering
- [ ] Multiple table support
- [ ] Transaction support with WAL (Write-Ahead Logging)
- [ ] Concurrent access with locks
- [ ] Index support beyond primary key
- [ ] Dynamic schema definition
- [ ] Query optimization

## 📚 Learning Resources

This project implements concepts from:

- ["Let's Build a Simple Database" Tutorial](https://cstack.github.io/db_tutorial/)
- "Database System Concepts" by Silberschatz, Galvin, and Gagne
- B-tree algorithms and data structures

## 🤝 Contributing

This is primarily an educational project, but feedback and suggestions are welcome! Areas of interest:

- Performance optimizations
- Additional SQL features
- Better error handling
- Code organization improvements

## 📄 License

[Your chosen license - MIT, Apache 2.0, etc.]

## 🙏 Acknowledgments

- Original C tutorial by Connor Stack
- The Rust community for excellent documentation
- Database systems textbooks and research papers

---
