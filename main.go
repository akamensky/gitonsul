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
	addr   string
	token  string
	prefix string
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

	if err := p.Parse(os.Args); err != nil {
		exitWithError(err)
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
		addr:   *consulAddrArg,
		token:  *consulTokenArg,
		prefix: strings.Trim(*consulPrefixArg, "/"),
	}

	t := time.NewTicker(interval)
	for range t.C {
		// Get KV data as a map from Git repo
		kvData, err := repoGetKV(gitConf)
		if err != nil {
			exitWithError(err)
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
			exitWithError(err)
		}
	}
}

func exitWithError(err error) {
	slog.Error(err.Error())
	os.Exit(1)
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
		exitWithError(err)
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
	client, err := api.NewClient(&api.Config{
		Address: conf.addr,
		Token:   conf.token,
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
			if _, err = client.KV().Delete(key, nil); err != nil {
				return err
			}
		}
	}

	// Put keys into KV
	for key, value := range kvData {
		kvPair, _, err := client.KV().Get(key, nil)
		if err != nil {
			return err
		}

		if kvPair != nil && reflect.DeepEqual(kvPair.Value, value) {
			continue
		}

		_, err = client.KV().Put(&api.KVPair{
			Key:         key,
			CreateIndex: 0,
			ModifyIndex: 0,
			LockIndex:   0,
			Flags:       0,
			Value:       value,
			Session:     "",
			Namespace:   "",
			Partition:   "",
		}, nil)
		if err != nil {
			return err
		}
	}

	return nil
}
