package main

import (
	"fmt"
	"github.com/akamensky/argparse"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/hashicorp/consul/api"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

const descr = `Populate Hashicorp Consul KV from Git repository.
Path to file relative to root of repository used as key and file contents as value.`

type gitConfig struct {
	repo      string
	authBasic string
	authToken string
	authKey   string
	prefix    string
}

type consulConfig struct {
	addr        string
	token       string
	prefix      string
	datacenters []string
}

func main() {
	p := argparse.NewParser("gitonsul", descr)

	intervalArg := p.Int("i", "interval", &argparse.Options{
		Required: false,
		Help:     "Interval in seconds to check updates in Bitbucket",
		Default:  60,
	})

	gitRepoArg := p.String("", "git-repo", &argparse.Options{
		Required: true,
		Help:     "Git repository string. For SSH based access repositories make sure the SSH keys are configured as expected",
	})

	gitAuthBasicArg := p.String("", "git-auth-basic", &argparse.Options{
		Required: false,
		Help:     "Credentials when using Git over HTTP in format `username:password`",
		Default:  "",
	})

	gitAuthTokenArg := p.String("", "git-auth-token", &argparse.Options{
		Required: false,
		Help:     "Bearer token when using Git over HTTP",
		Default:  "",
	})

	gitAuthKeyArg := p.String("", "git-auth-key", &argparse.Options{
		Required: false,
		Help:     "Path to private key when using Git over SSH",
		Default:  "",
	})

	gitPrefixArg := p.String("", "git-prefix", &argparse.Options{
		Required: false,
		Help:     "Only use data from the provided prefix. Leading and trailing delimiters will be ignored",
		Default:  "",
	})

	consulAddrArg := p.String("", "consul-addr", &argparse.Options{
		Required: false,
		Help:     "Consul API address",
		Default:  "http://127.0.0.1:8500",
	})

	consulTokenArg := p.String("", "consul-token", &argparse.Options{
		Required: false,
		Help:     "Consul HTTP token with appropriate ACLs attached",
		Default:  "",
	})

	consulPrefixArg := p.String("", "consul-prefix", &argparse.Options{
		Required: false,
		Help:     "KV prefix to where write the data. Leading and trailing delimiters will be ignored",
		Default:  "",
	})

	consulDatacentersArg := p.String("", "consul-datacenters", &argparse.Options{
		Required: false,
		Help:     "A comma-delimited list of Consul datacenters to sync data to. By default it will sync to all datacenters",
		Default:  "",
	})

	if err := p.Parse(os.Args); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	interval := time.Duration(*intervalArg) * time.Second

	gitConf := &gitConfig{
		repo:      *gitRepoArg,
		authBasic: *gitAuthBasicArg,
		authToken: *gitAuthTokenArg,
		authKey:   *gitAuthKeyArg,
		prefix:    strings.Trim(*gitPrefixArg, "/"),
	}
	consulConf := &consulConfig{
		addr:        *consulAddrArg,
		token:       *consulTokenArg,
		prefix:      strings.Trim(*consulPrefixArg, "/"),
		datacenters: make([]string, 0),
	}
	if len(*consulDatacentersArg) > 0 {
		consulConf.datacenters = strings.Split(*consulDatacentersArg, ",")
		for i, datacenter := range consulConf.datacenters {
			consulConf.datacenters[i] = strings.TrimSpace(datacenter)
		}
	}

	if err := consulVerifyDCs(consulConf); err != nil {
		slog.Error("error verifying Consul Datacenters", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("consul setup", slog.Any("datacenters", consulConf.datacenters), slog.String("address", consulConf.addr), slog.String("prefix", consulConf.prefix))
	slog.Info("starting update loop", slog.Duration("interval", interval))

	t := time.NewTicker(interval)
	for range t.C {
		// Get KV data as a map from Git repo
		kvData, err := repoGetKV(gitConf)
		if err != nil {
			slog.Error("error getting KV data from repository", slog.Any("error", err))
			continue
		}
		// prepend prefix if any needed
		if consulConf.prefix != "" {
			for key, value := range kvData {
				delete(kvData, key)
				key = fmt.Sprintf("%s/%s", consulConf.prefix, key)
				kvData[key] = value
			}
		}
		// Iterate over the map
		if err = consulSetKV(consulConf, kvData); err != nil {
			slog.Error("error updating Consul KV data", slog.Any("error", err))
			continue
		}
	}
}

func repoGetKV(conf *gitConfig) (map[string][]byte, error) {
	result := make(map[string][]byte)

	// clone to in-memory
	opts := &git.CloneOptions{
		URL:          conf.repo,
		SingleBranch: true,
		Depth:        1,
	}
	if conf.authBasic != "" {
		parts := strings.SplitN(conf.authBasic, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid Basic Auth credentials")
		}
		opts.Auth = &http.BasicAuth{
			Username: parts[0],
			Password: parts[1],
		}
	} else if conf.authToken != "" {
		opts.Auth = &http.TokenAuth{
			Token: conf.authToken,
		}
	} else if conf.authKey != "" {
		pubKey, err := ssh.NewPublicKeysFromFile("git", conf.authKey, "")
		if err != nil {
			return nil, err
		}
		opts.Auth = pubKey
	}
	storage := memory.NewStorage()
	repo, err := git.Clone(storage, nil, opts)
	if err != nil {
		return nil, err
	}

	head, err := repo.Head()
	if err != nil {
		return nil, err
	}
	commit, err := repo.CommitObject(head.Hash())
	if err != nil {
		return nil, err
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, err
	}
	err = tree.Files().ForEach(func(file *object.File) error {
		if file != nil {
			key := strings.Trim(file.Name, "/")
			valueStr, err := file.Contents()
			if err != nil {
				return err
			}
			// filter by prefix
			if strings.HasPrefix(key, conf.prefix) {
				result[strings.Trim(strings.TrimPrefix(key, conf.prefix), "/")] = []byte(valueStr)
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func consulSetKV(conf *consulConfig, kvData map[string][]byte) error {
	wg := &sync.WaitGroup{}
	closeCh := make(chan interface{})
	var err error
	for _, datacenter := range conf.datacenters {
		wg.Add(1)
		go func(dc string) {
			defer wg.Done()

			if localErr := consulDcSetKv(dc, conf, kvData, closeCh); localErr != nil {
				close(closeCh)
				err = fmt.Errorf("error processing datacenter '%s': %w", dc, localErr)
			}
		}(datacenter)
	}
	wg.Wait()

	if err != nil {
		return err
	}

	return nil
}

func consulDcSetKv(datacenter string, conf *consulConfig, kvData map[string][]byte, closeCh <-chan interface{}) error {
	client, err := api.NewClient(&api.Config{
		Address:    conf.addr,
		Token:      conf.token,
		Datacenter: datacenter,
	})
	if err != nil {
		return err
	}

	// Delete all keys that should not exist
	keys, _, err := client.KV().Keys(conf.prefix, "", nil)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if _, ok := kvData[key]; !ok {
			select {
			case <-closeCh:
				return nil
			default:
				if _, err = client.KV().Delete(key, nil); err != nil {
					return err
				}
				slog.Info("key deleted", slog.String("key", key), slog.String("datacenter", datacenter))
			}
		}
	}

	// Put keys into KV
	for key, value := range kvData {
		select {
		case <-closeCh:
			return nil
		default:
			kvPair, _, err := client.KV().Get(key, nil)
			if err != nil {
				return err
			}

			if kvPair != nil && reflect.DeepEqual(kvPair.Value, value) {
				continue
			}

			pair := &api.KVPair{
				Key:   key,
				Value: value,
			}
			wo := &api.WriteOptions{
				Datacenter: datacenter,
			}

			_, err = client.KV().Put(pair, wo)
			if err != nil {
				return err
			}
			slog.Info("key created", slog.String("key", key), slog.String("datacenter", datacenter))
		}
	}

	return nil
}

// consulVerifyDCs will compare configuration to check if all provided DCs exists
// if no provided DCs then it will fill configuration with all existing Consul DCs
func consulVerifyDCs(conf *consulConfig) error {
	client, err := api.NewClient(&api.Config{
		Address: conf.addr,
		Token:   conf.token,
	})
	if err != nil {
		return err
	}

	consulDcList, err := client.Catalog().Datacenters()
	if err != nil {
		return err
	}
	if len(conf.datacenters) > 0 {
		// If we have any data in conf.datacenters,
		// then we compare and filter that
		consulDcMap := make(map[string]bool)
		for _, dc := range consulDcList {
			consulDcMap[dc] = true
		}

		for _, confDc := range conf.datacenters {
			if _, ok := consulDcMap[confDc]; !ok {
				return fmt.Errorf("configured Consul Datacenter '%s' does not exist; found %v", confDc, consulDcList)
			}
		}
	} else {
		conf.datacenters = consulDcList
	}

	return nil
}
