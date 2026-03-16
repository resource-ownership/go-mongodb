package mongodb

import (
	"reflect"

	shared "github.com/resource-ownership/go-common/pkg/common"
	"go.mongodb.org/mongo-driver/mongo"
)

// NewMongoDBRepository creates a new generic MongoDB repository instance
func NewMongoDBRepository[T shared.Entity](mongoClient *mongo.Client, dbName string, entity T, collectionName string, entityName string) *MongoDBRepository[T] {
	entityType := reflect.TypeOf(entity)
	if entityType.Kind() == reflect.Ptr {
		entityType = entityType.Elem()
	}

	return &MongoDBRepository[T]{
		mongoClient:     mongoClient,
		dbName:          dbName,
		mappingCache:    make(map[string]CacheItem),
		entityModel:     entityType,
		collectionName:  collectionName,
		entityName:      entityName,
		BsonFieldMappings: make(map[string]string),
		QueryableFields:   make(map[string]bool),
	}
}

// NewMongoDBRepositoryForType creates a new generic MongoDB repository instance without requiring a value copy of T.
func NewMongoDBRepositoryForType[T shared.Entity](mongoClient *mongo.Client, dbName string, collectionName string, entityName string) *MongoDBRepository[T] {
	entityType := reflect.TypeFor[T]()
	if entityType.Kind() == reflect.Ptr {
		entityType = entityType.Elem()
	}

	return &MongoDBRepository[T]{
		mongoClient:       mongoClient,
		dbName:            dbName,
		mappingCache:      make(map[string]CacheItem),
		entityModel:       entityType,
		collectionName:    collectionName,
		entityName:        entityName,
		BsonFieldMappings: make(map[string]string),
		QueryableFields:   make(map[string]bool),
	}
}