package redis

import (
	"github.com/go-redis/redis"
	"github.com/journeymidnight/seaweedfs/weed/filer2"
	"github.com/journeymidnight/seaweedfs/weed/util"
)

func init() {
	filer2.Stores = append(filer2.Stores, &RedisClusterStore{})
}

type RedisClusterStore struct {
	UniversalRedisStore
}

func (store *RedisClusterStore) GetName() string {
	return "redis_cluster"
}

func (store *RedisClusterStore) Initialize(configuration util.Configuration) (err error) {
	return store.initialize(
		configuration.GetStringSlice("addresses"),
		configuration.GetString("password"),
	)
}

func (store *RedisClusterStore) initialize(addresses []string, password string) (err error) {
	store.Client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addresses,
		Password: password,
	})
	return
}
