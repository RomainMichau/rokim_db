# Rokim DB - Project Documentation

## Overview

Rokim DB is a custom file-based database implementation written in Scala. It provides a simple database system with support for tables, typed columns, primary keys, auto-increment fields, and filtering capabilities.

## Project Structure

```
rokim_db/
├── src/
│   ├── main/scala/com/rokim/db/
│   │   ├── Domain/
│   │   │   ├── DbAdapter.scala       # Database adapter interface
│   │   │   ├── Filter.scala          # Filtering logic
│   │   │   └── Types.scala           # Type system (columns, values, etc.)
│   │   ├── adapters/fs/
│   │   │   └── DbAdapterImpl.scala   # File system implementation
│   │   ├── Utils.scala               # Byte conversion utilities
│   │   └── DataGenerator.scala       # Utility to generate test data
│   └── test/scala/com/rokim/db/
│       ├── adapters/fs/
│       │   └── DbAdaptorImplTest.scala  # Unit tests
│       └── DataQuerier.scala         # Utility to query and inspect data
└── test_dbs/                         # Test database storage
```

## Core Components

### Type System (`Types.scala`)

The database uses a strongly-typed system with the following supported types:

- **INT**: 4-byte integer
- **LONG**: 8-byte long integer
- **BOOLEAN**: 1-byte boolean
- **VARCHAR(n)**: Variable-length string with fixed byte size

Each type has:
- `byteSize`: Fixed size in bytes
- `valueOf(u: U)`: Create a value from Scala type
- `valueOf(u: Seq[Byte])`: Parse value from bytes

### Database Operations (`DbAdapter.scala`)

The `DbAdapter` trait defines the core operations:

```scala
trait DbAdapter {
  def createTable(tableName: String, columns: Seq[Column[?]],
                  pk: Seq[Column[?]], autoInc: Option[Column[?]]): Unit
  def insertData(tableName: String, value: Seq[ColumnValue[?]]): Unit
  def getAll(tableName: String): Seq[LineValue]
  def getFiltered(tableName: String, filter: Filter): Seq[LineValue]
}
```

### Filtering (`Filter.scala`)

Supports filtering with various operators:
- **IntColumnFilter**: Filter integer columns (Greater, Equals, etc.)
- **BooleanColumnFilter**: Filter boolean columns (Equals)
- More filter types can be added as needed

### File System Adapter (`DbAdapterImpl.scala`)

Implements `DbAdapter` using file-based storage:
- **Metadata file**: Stores table schema, primary keys, and auto-increment state
- **Data file**: Stores row data as binary

## Data Model

### Column Definition

```scala
val idCol = Column("id", LONG)
val ageCol = Column("age", INT)
val nameCol = Column("name", VARCHAR(50))
val activeCol = Column("active", BOOLEAN)
```

### Creating a Table

```scala
val adapter = new DbAdapterImpl(Paths.get("test_dbs"))
adapter.createTable(
  tableName = "people",
  columns = Seq(idCol, ageCol, nameCol, activeCol),
  pk = Seq(idCol),
  autoInc = Some(idCol)  // Optional auto-increment
)
```

### Inserting Data

```scala
adapter.insertData("people", Seq(
  idCol.valueOf(1L),
  ageCol.valueOf(25),
  nameCol.valueOf("John Doe"),
  activeCol.valueOf(true)
))
```

### Querying Data

```scala
// Get all rows
val allData = adapter.getAll("people")

// Filter by age > 30
val ageFilter = IntColumnFilter("age", Greater[Int](), 30)
val filtered = adapter.getFiltered("people", ageFilter)

// Filter by active status
val activeFilter = BooleanColumnFilter("active", Equals(), true)
val activeUsers = adapter.getFiltered("people", activeFilter)
```

### Reading Values

```scala
val line: LineValue = allData.head
val id = line.getColumn("id").value        // Returns the value
val age = line.getColumn("age").value.asInstanceOf[Int]
val name = line.getColumn("name").value.asInstanceOf[String]
val active = line.getColumn("active").value.asInstanceOf[Boolean]
```

## Utilities

### DataGenerator

Generates test data for the database:

```bash
# Generate 1000 entries (default)
sbt "runMain com.rokim.db.DataGenerator"

# Generate 5000 entries
sbt "runMain com.rokim.db.DataGenerator 5000"

# Custom location and table
sbt "runMain com.rokim.db.DataGenerator 10000 my_dbs my_table"
```

### DataQuerier

Query and inspect database contents:

```bash
# Query default database (test_dbs/people)
sbt "Test/runMain com.rokim.db.DataQuerier"

# Query specific database
sbt "Test/runMain com.rokim.db.DataQuerier test_dbs people"
```

Features:
- Shows first/last entries
- Filters by age thresholds
- Filters by active status
- Calculates statistics (avg/min/max age, active percentage)

## Storage Format

### Metadata File

```
<table_name>
AUTOINC <column_name> <current_count>
<pk_column_1>
<pk_column_2>
...
<column_1_name> <column_1_type> [size]
<column_2_name> <column_2_type> [size]
...
```

Example:
```
people
AUTOINC id 67
id
id LONG
age INT
name VARCHAR 50
active BOOLEAN
```

### Data File

Binary format with fixed-width rows:
- Each row contains all column values concatenated
- Values are stored in column definition order
- Each value uses its type's `byteSize`

## Testing

Run all tests:
```bash
sbt test
```

Run specific test:
```bash
sbt "testOnly com.rokim.db.adapters.fs.DbAdaptorImplTest"
```

## Current Limitations

- No indexing (full table scans for queries)
- Limited filter types (only INT and BOOLEAN filters implemented)
- No transaction support
- No concurrent access handling
- Fixed-width VARCHAR storage (padding applied)
- Single table queries only (no joins)

## Future Enhancements

Potential improvements:
- Add more filter operators (LessThan, Between, NotEquals, etc.)
- Implement indexing for faster queries
- Add support for LIKE queries on VARCHAR
- Implement UPDATE and DELETE operations
- Add transaction support
- Support for NULL values
- Composite filters (AND, OR)
- Query optimization