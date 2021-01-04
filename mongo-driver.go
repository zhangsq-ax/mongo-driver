package mongo_driver

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sort"
	"strings"
	"time"
)

type MongoDriver struct {
	client *mongo.Client
	db     *mongo.Database
}

type MongoDriverOptions struct {
	Database string
	Host     string
	Port     int
	Username string
	Password string
}

type IndexOption struct {
	Name   string
	Keys   map[string]int
	Unique bool
}

type ListOption struct {
	Filter bson.M
	Sorter bson.D
	Limit  int64
	Skip   int64
}

func NewMongoDriver(opts MongoDriverOptions) (*MongoDriver, error) {
	client, err := connect(fmt.Sprintf("mongodb://%s:%s@%s:%d", opts.Username, opts.Password, opts.Host, opts.Port))
	if err != nil {
		return nil, err
	}

	return &MongoDriver{
		client: client,
		db:     client.Database(opts.Database),
	}, nil
}

func (d *MongoDriver) GetCollection(name string) *mongo.Collection {
	return d.db.Collection(name)
}

func connect(mongoUri string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(mongoUri)

	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getCollectionIndexes(c *mongo.Collection) ([]bson.M, error) {
	iv := c.Indexes()
	cursor, err := iv.List(context.Background(), options.ListIndexes().SetMaxTime(2*time.Second))
	if err != nil {
		return nil, err
	}
	var results []bson.M
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func hasIndex(c *mongo.Collection, idxName string) (bool, error) {
	r, err := getCollectionIndexes(c)
	if err != nil {
		return false, err
	}

	for _, idx := range r {
		if idx["name"] == idxName {
			return true, nil
		}
	}

	return false, nil
}

func generateIndexName(opts *IndexOption) {
	if opts.Name == "" {
		var fields []string
		for field, _ := range opts.Keys {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		opts.Name = fmt.Sprintf("idx_%s", strings.Join(fields, "_"))
	}
}

func CreateIndex(c *mongo.Collection, opts ...*IndexOption) error {
	for _, opt := range opts {
		generateIndexName(opt)
		exists, err := hasIndex(c, opt.Name)
		if err != nil {
			return err
		}
		if !exists {
			opts := options.Index()
			opts.SetUnique(opt.Unique).SetName(opt.Name)
			im := mongo.IndexModel{
				Keys:    opt.Keys,
				Options: opts,
			}
			iv := c.Indexes()
			str, err := iv.CreateOne(context.Background(), im)
			log.Println(str)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func RemoveIndex(c *mongo.Collection, indexNames ...string) error {
	iv := c.Indexes()
	for _, indexName := range indexNames {
		_, err := iv.DropOne(context.Background(), indexName)
		if err != nil {
			return err
		}
	}
	return nil
}

func RemoveIndexByOption(c *mongo.Collection, opts ...*IndexOption) error {
	var indexNames []string
	for _, opt := range opts {
		generateIndexName(opt)
		indexNames = append(indexNames, opt.Name)
	}

	return RemoveIndex(c, indexNames...)
}

func List(c *mongo.Collection, opt *ListOption, results interface{}) error {
	opts := options.Find()
	if opt.Limit > 0 && opt.Skip > 0 {
		opts.SetLimit(opt.Limit).SetSkip(opt.Skip)
	}
	if opt.Sorter != nil {
		opts.SetSort(opt.Sorter)
	}

	cursor, err := c.Find(context.Background(), opt.Filter, opts)
	if err != nil {
		return err
	}

	return cursor.All(context.Background(), results)
}
