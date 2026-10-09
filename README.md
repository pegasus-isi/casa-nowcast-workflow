# casa-nowcast-workflow

Pegasus workflow for CASA radar nowcasting. For each 30-minute nowcast
forecast file (`MERGE_DARTS_YYYYMMDDHHMM.nc`), it:

1. splits the forecast into 31 per-minute reflectivity files (`NowcastToWDSS2`)
2. thresholds each into a storm GeoJSON (`mrtV2`)
3. renders each into a PNG (`merged_netcdf2png`)

Jobs run in an Apptainer image built from `pegasus/casa-nowcast` (see
[Container](#container)) on a site named `compute` (see [Sites](#sites)). Sample forecast files ship in `input/`.

## Running the workflow

On a submit host with Pegasus, HTCondor and Apptainer:

```bash
# 1. Build the container once (Linux, x86_64)
apptainer build Apptainer/CASA_Nowcast_Container.sif Apptainer/CASA_Nowcast_Container.def

# 2a. With a hosted site catalog (or one set in ~/.pegasusrc):
./workflow_generator.py -f MERGE_DARTS_201903250500.nc -s access-pegasus.yml
./plan.sh

# 2b. Or on a plain HTCondor pool with no site catalog:
./workflow_generator.py -f MERGE_DARTS_201903250500.nc -e condorpool
SITE=condorpool ./plan.sh
```

Final outputs land in `wf-output/` (Pegasus's default output location when
no catalog defines a `local` site). Or open `CASA-Nowcast-Workflow.ipynb` and run its cells: it uses
the same generator class, submits from an explicit cell, and stages outputs to
`output/`.

## Usage

Generate `workflow.yml` (transformation and replica catalogs embedded) and
`pegasus.properties`:

```sh
./workflow_generator.py -f MERGE_DARTS_201903250500.nc
```

`-f` accepts one or more forecast files (bare names are resolved in
`./input/`); passing several combines them into a single workflow. `-o` sets where `workflow.yml` and `pegasus.properties` are
written (default: project root).

The generator does not plan or submit; it prints the command to do so. Plan
and submit with it, or with `plan.sh`, which runs the same command:

```sh
./plan.sh                       # site "compute"
SITE=condorpool ./plan.sh       # plain HTCondor pool, no site catalog
```

### Notebook

`CASA-Nowcast-Workflow.ipynb` drives the same generator class interactively: it builds the
container, generates the workflow, shows the DAG, then plans, submits and
monitors it from explicit cells. It contains no workflow code of its own —
change the pipeline in `workflow_generator.py`.

## Container

The tools ship prebuilt in the `pegasus/casa-nowcast` Docker image.
`Apptainer/CASA_Nowcast_Container.def` converts it to a local `.sif` that Pegasus stages to
the workers, so jobs need no registry access. Build it once on the submit
host (Linux, x86_64) before generating the workflow:

```sh
apptainer build Apptainer/CASA_Nowcast_Container.sif Apptainer/CASA_Nowcast_Container.def
```

`--container-sif PATH` points the generator at an image elsewhere.

## Sites

Jobs run on a site named `compute`, which the generator never writes: where
they run depends on your resource provider and allocation, so it comes from a
site catalog you choose (the
[pegasus-gromacs](https://github.com/pegasus-isi/pegasus-gromacs) pattern).

- **Hosted catalog:** `-s access-pegasus.yml` (or `unity.yml`,
  `nersc-perlmutter.yml`, ... from
  [pegasus-site-catalogs](https://github.com/pegasushub/pegasus-site-catalogs/tree/main/conf))
  writes `pegasus.catalog.site.repo.file` into `pegasus.properties`; set it
  once in `~/.pegasusrc` instead to apply it to every workflow.
- **Plain HTCondor pool with no site catalog:** generate with
  `-e condorpool` and plan with `-s condorpool` — Pegasus builds that site
  itself.

| Option | Default | Description |
|--------|---------|-------------|
| `-f`, `--files` | (required) | Input files, bare names resolved in `./input/` |
| `-o`, `--outdir` | `.` | Where `workflow.yml` and `pegasus.properties` are written |
| `--container-sif` | `Apptainer/CASA_Nowcast_Container.sif` | Apptainer image |
| `-s`, `--hosted-site-catalog` | (none; `~/.pegasusrc` if set) | Hosted site catalog to plan against |
| `-e`, `--execution-site-name` | `compute` | Execution site name; `condorpool` on a plain HTCondor pool with no site catalog |

## PegasusAI Studio

This repo is Studio-compatible: clone it into `~/work/workflows/`, and it
appears on the Dashboard. The Generate action introspects
`workflow_generator.py` (declared in `.pegasushub.yml`), and Plan/Submit work
directly on the generated `workflow.yml`.

## Legacy files

`legacy/` holds the pre-5.0-style generator and external catalogs
(`workflow.py`, `daxgen.py`, `run_wf.sh`, `sites.xml`, `tc.txt`, `rc.txt`),
superseded by `workflow_generator.py`. They must stay out of the project
root: pegasus-plan picks up `tc.txt`/`rc.txt`/`sites.xml` from the working
directory by default, and those definitions silently shadow the catalogs
embedded in `workflow.yml` (e.g. reverting the container to Docker).