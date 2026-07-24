# casa-nowcast-workflow

Pegasus workflow for CASA radar nowcasting. For each 30-minute nowcast
forecast file (`MERGE_DARTS_YYYYMMDDHHMM.nc`), it:

1. splits the forecast into 31 per-minute reflectivity files (`NowcastToWDSS2`)
2. thresholds each into a storm GeoJSON (`mrtV2`)
3. renders each into a PNG (`merged_netcdf2png`)

All jobs run in the `pegasus/casa-nowcast` Docker container on a site named
`condorpool`. Sample forecast files ship in `input/`.

## Usage

Generate `workflow.yml` (catalogs embedded — no external `sites.xml`/`tc.txt`/
`rc.txt` needed):

```sh
./workflow_generator.py -f MERGE_DARTS_201903250500.nc
```

`-f` accepts one or more forecast files (bare names are resolved in
`./input/`); passing several combines them into a single workflow. `-o` sets
where `workflow.yml` is written (default: project root).

Plan and submit:

```sh
./plan.sh                 # plans + submits ./workflow.yml
pegasus-status -l submit/<user>/pegasus/<wf-name>/<run>
```

or equivalently `pegasus-plan -s condorpool -o local --submit workflow.yml`.

## PegasusAI Studio

This repo is Studio-compatible: clone it into `~/work/workflows/`, and it
appears on the Dashboard. The Generate action introspects
`workflow_generator.py` (declared in `.pegasushub.yml`), and Plan/Submit work
directly on the generated `workflow.yml`.

## Legacy files

`workflow.py`, `daxgen.py`, `run_wf.sh`, `sites.xml`, `tc.txt`, and `rc.txt`
are the pre-5.0-style generator and external catalogs, superseded by
`workflow_generator.py`; they are kept for reference.