# gitonsul
Populate Consul KV from Git repository

## Usage
Check built-in help message to see the latest usage instructions:
```
$ gitonsul --help
... 
```
Example usage:
```
$ gitonsul \
    --git-repo https://github.com/akamensky/gitonsul.git \
    --consul-addr http://some_host:8500
```
Using Docker:
```
$ docker run ghcr.io/akamensky/gitonsul:latest \
    --git-repo https://github.com/akamensky/gitonsul.git \
    --consul-addr http://some_host:8500
```
