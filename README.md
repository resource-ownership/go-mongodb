# go-mongodb

A generic MongoDB repository package providing reusable database operations for Go applications.

## Overview

This package provides a generic `MongoDBRepository[T]` that implements common CRUD operations, advanced querying with search parameters, field projections, sorting, pagination, and row-level security (RLS) for multi-tenant applications.

## Features

- **Generic Repository**: Type-safe repository pattern for any entity type
- **Advanced Querying**: Support for complex search parameters with operators (equals, contains, in, etc.)
- **Field Projection**: Selective field retrieval with pick/omit functionality
- **Sorting & Pagination**: Configurable sorting and pagination with limits
- **Row-Level Security**: Built-in tenant isolation and access control
- **UUID Support**: Custom BSON codec for UUID handling
- **Aggregation Pipeline**: MongoDB aggregation pipeline support for complex queries

## Installation

```bash
go get github.com/resource-ownership/go-mongodb
```

## Usage

### Basic Setup

```go
import (
    "github.com/resource-ownership/go-mongodb/pkg/mongodb"
    shared "github.com/resource-ownership/go-common/pkg/common"
)

// Define your entity
type User struct {
    ID   uuid.UUID `bson:"_id"`
    Name string    `bson:"name"`
    // ... other fields
}

func (u User) GetID() uuid.UUID { return u.ID }

// Create repository
repo := mongodb.NewMongoDBRepository[User](mongoClient, "database", User{}, "users", "User")

// Initialize queryable fields and mappings
repo.InitQueryableFields(
    map[string]bool{
        "ID":   true,
        "Name": true,
    },
    map[string]string{
        "ID":   "_id",
        "Name": "name",
    },
)
```

### CRUD Operations

```go
// Create
user, err := repo.Create(ctx, &User{Name: "John"})

// Get by ID
user, err := repo.GetByID(ctx, userID)

// Update
updatedUser, err := repo.Update(ctx, user)

// Search with parameters
search := shared.NewSearchByParams(ctx, []shared.SearchAggregation{
    {
        Params: []shared.SearchParameter{
            {
                ValueParams: []shared.SearchableValue{
                    {Field: "Name", Operator: shared.ContainsOperator, Values: []interface{}{"John"}},
                },
            },
        },
    },
}, shared.SearchResultOptions{Limit: 10})

users, err := repo.Search(ctx, search)
```

### Advanced Features

#### Row-Level Security

The repository automatically enforces tenant isolation based on context values.

#### Aggregation Queries

Complex queries with joins, filtering, and projections are supported through the aggregation pipeline.

#### Custom BSON Field Mapping

Map Go struct field names to MongoDB BSON field names for flexible schema design.

## Dependencies

- `github.com/resource-ownership/go-common`: Common types and interfaces
- `go.mongodb.org/mongo-driver`: MongoDB Go driver
- `github.com/google/uuid`: UUID support

## Testing

```bash
go test ./...
```

## Contributing

This package is designed to be a reusable component across multiple repositories. When making changes:

1. Ensure backward compatibility
2. Add tests for new functionality
3. Update documentation
4. Consider the impact on existing consumers

## License

See LICENSE file for details.
