# Cabin

This repo contains Cabin, a tool to facilitate the versioning of database tables.


## Dev Environment Setup
Assuming linux machine, macOS may need to install some extras such as wget (through brew or from source).

1. Set up the repo:
```
git clone github.com/seeqbio/cabin.git
cd cabin
make venv
source venv/bin/activate
```

2. Set MySQL: 
To get a dev environment up and running for mysql, either (1) run `bash mysql/init.sh`. This will run the officialy MySQL Docker image and create a Cabin database and user on it. **Or** if you have mysql already set up (2) add the name of the database you wish to use, the user, and password to the environmental variables in `cabin/settings.py`. Then run `cabin init` to create a `cabin_system` table.

3. Quick Start:
Below are commands to run to import the example data provided. See usage examples and documentation for more details.

```
(venv) > bin/cabin list
(venv) > bin/cabin status
(venv) > bin/cabin import --all --dry-run
(venv) > bin/cabin import --all
(venv) > bin/cabin status
(venv) > bin/cabin shell
```

## Usage examples
*The following is an example, with detailed output, of how a dataset would be imported and updated*

**See which datasets are available to work with, import all.** Note that:

* the `import` command may specify a dataset directly or use `--all` to import all possible datasets that are not already imported
* given the one example, `import --all` is effectively the same as specifying `import StormDetailsTable` because all dependancies of the table are verified either way
* `import StormDetailsFile` however would only have triggered the download of the csv to the downloads directory
* `--dry-run` allows a preview of which datasets will be affected

```
(venv) $ bin/cabin list
StormDetailsFile
StormDetailsOfficial
StormDetailsTable

(venv) $ bin/cabin import --all
...
[2022-01-13 14:08:16,261] INFO produced: StormDetailsTable              with formula sha ce78a5c3 and root versions 2011

(venv) $ bin/cabin status
| version | table                             |   rows |    size | inputs               | outputs |
|--------:|:----------------------------------|-------:|--------:|:---------------------|:--------|
|    1  ✓ | StormDetailsTable::2011::ce78a5c3 | 79,101 | 19.4 MB | (1) StormDetailsFile | (0)     |
```

**If the root version were to change to 2012, we can update the dataset.** Note that:

* a manual change to `cabin/datasets/StormDetails.py` is required to update the version of the source data from "2011" to "2012"
* this change immediately renders the imported table (with data from 2011) outdated, reflected in status
* an `import` would trigger a new file to be downloaded from the external source to the local downloads dir

```
(venv) $ bin/cabin status
| version | table                             |   rows |    size | inputs               | outputs |
|--------:|:----------------------------------|-------:|--------:|:---------------------|:--------|
|    1  ! | StormDetailsTable::2011::ce78a5c3 | 79,101 | 19.4 MB | (1) StormDetailsFile | (0)     |
```
**aside** it is not necessary to manually update the version of the StormDetailsTable dataset because its import logic has not changed. We leave this to the user to decide as it is purely a communication tool to other developers to increase the version in downstream datasets or not. I have chosen to do so here because I find it communicates the idea more clearly for beginers - however leaving the version of StormDetailsTable as `1` would be perfectly reasonable and would all the same functionality.

**prune to remove tables that are not latest** Note that:
* `drop` is a command that can specify datasets, eg: `drop StormDetailsTable::2011::ce78a5c3` or `drop Storm*` for tables that are latest or not latest
* prune is more of a garbage cleanup

```
(venv) $ bin/cabin prune --dry-run
[2022-01-13 14:14:32,151] INFO (dry-run) Pruning outdated table: StormDetailsTable::2011::ce78a5c3
[2022-01-13 14:14:32,152] INFO (dry-run) Pruning outdated table: StormDetailsTable::2012::00ea4b32

(venv) $ bin/cabin status
| version | table                             |   rows |    size | inputs               | outputs |
|--------:|:----------------------------------|-------:|--------:|:---------------------|:--------|
|    1  ! | StormDetailsTable::2011::ce78a5c3 | 79,101 | 19.4 MB | (1) StormDetailsFile | (0)     |
|    2  ! | StormDetailsTable::2012::00ea4b32 | 64,508 | 15.2 MB | (1) StormDetailsFile | (0)     |
|    3  ✓ | StormDetailsTable::2012::d6e14f82 | 64,508 | 15.2 MB | (1) StormDetailsFile | (0)     |

(venv) $ bin/cabin prune
[2022-01-13 14:14:39,137] INFO Pruning outdated table: StormDetailsTable::2011::ce78a5c3
[2022-01-13 14:14:39,186] INFO Pruning outdated table: StormDetailsTable::2012::00ea4b32

(venv) $ bin/cabin status
| version | table                             |   rows |    size | inputs               | outputs |
|--------:|:----------------------------------|-------:|--------:|:---------------------|:--------|
|    3  ✓ | StormDetailsTable::2012::d6e14f82 | 64,508 | 15.2 MB | (1) StormDetailsFile | (0)     |
```
