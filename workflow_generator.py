#!/usr/bin/env python3
"""CASA Nowcast workflow generator (Pegasus 5).

Generates workflow.yml with the transformation and replica catalogs embedded,
plus pegasus.properties, so it plans with a plain
`pegasus-plan --conf pegasus.properties -s compute -o local workflow.yml` — no
external tc.txt/rc.txt and no --input-dir.

Sites follow pegasus-isi/pegasus-gromacs: jobs run on a site named "compute",
defined by a centrally hosted site catalog (-s access-pegasus.yml, ...;
https://github.com/pegasushub/pegasus-site-catalogs) or by one in
~/.pegasusrc. The generator writes no site catalog; on a plain HTCondor pool
with no site catalog, use -e condorpool (Pegasus builds that site itself).
It does not plan or submit: run the pegasus-plan command it prints, or use
CASA-Nowcast-Workflow.ipynb.

For each 30-minute nowcast forecast file (MERGE_DARTS_YYYYMMDDHHMM.nc):
  1. NowcastToWDSS2 splits it into 31 per-minute reflectivity files
  2. mrtV2 thresholds each into a storm GeoJSON
  3. merged_netcdf2png renders each into a PNG
"""

import os
import re
import sys
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path

from Pegasus.api import (
    OS,
    Arch,
    Container,
    Directory,
    File,
    FileServer,
    Job,
    Namespace,
    Operation,
    PegasusClientError,
    Properties,
    ReplicaCatalog,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)

# Per-tool resource configuration for the transformation catalog.
TOOL_CONFIGS = {
    "NowcastToWDSS2": {"memory": "2 GB", "cores": 1},
    "mrtV2": {"memory": "2 GB", "cores": 1},
    "merged_netcdf2png": {"memory": "2 GB", "cores": 1},
}


class CASAWorkflow(object):
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    def __init__(self, outdir, forecast_files,
                 container_sif="Apptainer/CASA_Nowcast_Container.sif"):
        self.outdir = outdir
        self.forecast_files = forecast_files
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.container_sif = container_sif
        self.shared_scratch_dir = os.path.join(outdir, "scratch")
        self.local_storage_dir = os.path.join(outdir, "output")
        self.dagfile = os.path.join(outdir, "workflow.yml")
        self.propsfile = os.path.join(outdir, "pegasus.properties")

    def write(self):
        """Write workflow.yml (catalogs embedded) and pegasus.properties."""
        if self.sc is not None:
            self.wf.add_site_catalog(self.sc)
        self.wf.add_transformation_catalog(self.tc)
        self.wf.add_replica_catalog(self.rc)
        self.props.write(self.propsfile)
        self.wf.write(self.dagfile)

    # ------------------------------------------------------------------
    # Plan / run / monitor (thin wrappers over the Pegasus API Workflow
    # object, for interactive use e.g. from a Jupyter notebook)
    # ------------------------------------------------------------------
    def plan_submit(self, exec_site_name="compute", raise_errors=False):
        try:
            self.wf.plan(
                conf=self.propsfile,
                dir=os.path.join(self.outdir, "submit"),
                sites=[exec_site_name],
                output_sites=["local"],
                cleanup="leaf",
                verbose=1,
                submit=True,
            )
        except PegasusClientError as e:
            print(e)
            if raise_errors:
                raise

    def status(self):
        try:
            self.wf.status(long=True)
        except PegasusClientError as e:
            print(e)

    def wait(self):
        try:
            self.wf.wait()
        except PegasusClientError as e:
            print(e)

    def statistics(self):
        try:
            self.wf.statistics()
        except PegasusClientError as e:
            print(e)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    def create_pegasus_properties(self, hosted_site_catalog=None):
        self.props = Properties()
        # This is the name of the application for analytics
        self.props["pegasus.metrics.app"] = "CASA-NOWCAST"
        self.props["pegasus.transfer.threads"] = "16"
        if hosted_site_catalog:
            # Use one of Pegasus' centrally hosted site catalogs instead of
            # a locally generated one. pegasus-plan downloads and caches the
            # named file from the catalog repository at plan time.
            # https://pegasus.isi.edu/documentation/reference-guide/catalogs.html#centrally-hosted-site-catalogs
            self.props["pegasus.catalog.site.repo.file"] = hosted_site_catalog

    # ------------------------------------------------------------------
    # Site Catalog
    #
    # Not used by the CLI below — pegasus-plan resolves the site catalog
    # from a centrally hosted one instead (see -s/--hosted-site-catalog and
    # create_pegasus_properties above). Kept for programmatic/notebook use
    # when a self-contained, locally generated HTCondor site catalog is
    # wanted.
    # ------------------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="compute"):
        self.sc = SiteCatalog()

        local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX)
        local.add_directories(
            Directory(
                Directory.SHARED_SCRATCH, self.shared_scratch_dir
            ).add_file_servers(
                FileServer("file://" + self.shared_scratch_dir, Operation.ALL)
            ),
            Directory(
                Directory.LOCAL_STORAGE, self.local_storage_dir
            ).add_file_servers(
                FileServer("file://" + self.local_storage_dir, Operation.ALL)
            ),
        )

        exec_site = Site(exec_site_name, arch=Arch.X86_64, os_type=OS.LINUX)
        exec_site.add_condor_profile(universe="vanilla")
        exec_site.add_pegasus_profile(style="condor", data_configuration="condorio")

        self.sc.add_sites(local, exec_site)

    # ------------------------------------------------------------------
    # Transformation Catalog
    # ------------------------------------------------------------------
    def create_transformation_catalog(self, exec_site_name="compute"):
        self.tc = TransformationCatalog()

        # A local Apptainer .sif built with `apptainer build` from
        # Apptainer/CASA_Nowcast_Container.def. Pegasus stages the file like
        # any other input, so image_site is the site where the .sif
        # physically lives (the submit host = "local").
        sif_path = (
            self.container_sif
            if os.path.isabs(self.container_sif)
            else os.path.join(self.wf_dir, self.container_sif)
        )
        if not os.path.exists(sif_path):
            print(
                f"warning: Apptainer image not found at {sif_path} — build it "
                f"first with: apptainer build {sif_path} "
                f"Apptainer/CASA_Nowcast_Container.def",
                file=sys.stderr,
            )
        nowcast_container = Container(
            "nowcast_image",
            container_type=Container.SINGULARITY,
            image="file://" + sif_path,
            image_site="local",
        )
        self.tc.add_containers(nowcast_container)

        for name, pfn in (
            ("NowcastToWDSS2", "/opt/NowcastToWDSS2/NowcastToWDSS2"),
            ("mrtV2", "/opt/mrtV2/mrtV2"),
            ("merged_netcdf2png", "/opt/netcdf2png/merged_netcdf2png"),
        ):
            self.tc.add_transformations(
                Transformation(
                    name,
                    site=exec_site_name,
                    pfn=pfn,
                    is_stageable=False,
                    container=nowcast_container,
                    arch=Arch.X86_64,
                    os_type=OS.LINUX,
                ).add_pegasus_profile(
                    memory=TOOL_CONFIGS[name]["memory"],
                    cores=TOOL_CONFIGS[name]["cores"],
                )
            )

    # ------------------------------------------------------------------
    # Replica Catalog
    # ------------------------------------------------------------------
    def create_replica_catalog(self):
        self.rc = ReplicaCatalog()
        input_dir = os.path.join(self.wf_dir, "input")
        for aux in ("mrtV2_config.txt", "nexrad_ref.png"):
            self.rc.add_replica("local", aux, "file://" + os.path.join(input_dir, aux))

        for path in self.forecast_files:
            self.rc.add_replica("local", os.path.basename(path), "file://" + path)

    # ------------------------------------------------------------------
    # Workflow DAG
    # ------------------------------------------------------------------
    def create_workflow(self):
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.wf = wf = Workflow("casa-nowcast-wf-%s" % ts)
        wf.add_metadata(name="CASA Nowcast")

        mrtconfigfile = File("mrtV2_config.txt")
        colorscale = File("nexrad_ref.png")

        for path in self.forecast_files:
            fn = os.path.basename(path)

            # The 12-digit timestamp before the extension names the outputs
            # (the container tools derive their output names the same way).
            m = re.search(r"(\d{12})\.", fn)
            if not m:
                raise SystemExit(
                    f"error: cannot find YYYYMMDDHHMM timestamp in '{fn}' "
                    "(expected e.g. MERGE_DARTS_201903250500.nc)"
                )
            file_time = m.group(1) + "00"
            file_ymd = file_time[0:8]
            file_hms = file_time[8:14]

            # split the forecast into individual minute files
            forecast_file = File(fn)
            nowcast_split_job = Job("NowcastToWDSS2")
            nowcast_split_job.add_args(fn, ".")
            nowcast_split_job.add_inputs(forecast_file)
            pr_files = []
            for x in range(31):
                pr_file = File(
                    f"PredictedReflectivity_{x}min_{file_ymd}-{file_hms}.nc"
                )
                pr_files.append(pr_file)
                nowcast_split_job.add_outputs(
                    pr_file, stage_out=True, register_replica=False
                )
            wf.add_jobs(nowcast_split_job)

            for x, pr_file in enumerate(pr_files):
                # merged reflectivity threshold -> storm GeoJSON
                pr_geojson = File(
                    f"mrt_STORM_CASA_{x}_{file_ymd}-{file_hms}.geojson"
                )
                mrt_job = Job("mrtV2")
                mrt_job.add_args("-c", mrtconfigfile, pr_file.lfn)
                mrt_job.add_inputs(mrtconfigfile, pr_file)
                mrt_job.add_outputs(
                    pr_geojson, stage_out=True, register_replica=False
                )
                mrt_job.add_profiles(
                    Namespace.PEGASUS, "label", f"label_{fn}_{x}"
                )
                wf.add_jobs(mrt_job)

                # max reflectivity -> PNG
                pr_image = File(
                    f"PredictedReflectivity_{x}min_{file_ymd}-{file_hms}.png"
                )
                pr_image_job = Job("merged_netcdf2png")
                pr_image_job.add_args(
                    "-c", colorscale, "-q 235 -z 0,75", "-o", pr_image, pr_file
                )
                pr_image_job.add_inputs(pr_file, colorscale)
                pr_image_job.add_outputs(
                    pr_image, stage_out=True, register_replica=False
                )
                pr_image_job.add_profiles(
                    Namespace.PEGASUS, "label", f"label_{fn}_{x}"
                )
                wf.add_jobs(pr_image_job)


def resolve_forecast_file(name):
    """Absolute path of a forecast file given bare name or path."""
    if os.path.isfile(name):
        return os.path.abspath(name)
    candidate = os.path.join("input", os.path.basename(name))
    if os.path.isfile(candidate):
        return os.path.abspath(candidate)
    raise SystemExit(
        f"error: forecast file '{name}' not found (looked in ./input/)"
    )


def main():
    parser = ArgumentParser(
        description="CASA Nowcast Workflow",
        epilog="Writes workflow.yml and pegasus.properties; it does not plan "
        "or submit. Run the pegasus-plan command it prints, or use "
        "CASA-Nowcast-Workflow.ipynb.",
    )
    parser.add_argument(
        "-f",
        "--files",
        metavar="INPUT_FILE",
        type=str,
        nargs="+",
        help="Forecast file(s), e.g. MERGE_DARTS_201903250500.nc "
        "(bare names are resolved in ./input/)",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--outdir",
        metavar="OUTPUT_LOCATION",
        type=str,
        default=".",
        help="Directory to write workflow.yml and pegasus.properties into "
        "(default: project root)",
    )
    parser.add_argument(
        "--container-sif",
        metavar="PATH",
        type=str,
        default="Apptainer/CASA_Nowcast_Container.sif",
        help="Path to the Apptainer .sif image, absolute or relative to the "
        "workflow directory (default: Apptainer/CASA_Nowcast_Container.sif)",
    )
    parser.add_argument(
        "-s",
        "--hosted-site-catalog",
        metavar="FILE",
        type=str,
        default=None,
        help="Name of a Pegasus centrally hosted site catalog to plan against "
        "(e.g. access-pegasus.yml), instead of a locally generated one. Sets "
        "pegasus.catalog.site.repo.file; see "
        "https://pegasus.isi.edu/documentation/reference-guide/catalogs.html"
        "#centrally-hosted-site-catalogs",
    )
    parser.add_argument(
        "-e",
        "--execution-site-name",
        metavar="STR",
        type=str,
        default="compute",
        help="Execution site name (default: compute; use condorpool on a "
        "plain HTCondor pool with no site catalog)",
    )

    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    forecast_files = [resolve_forecast_file(f) for f in args.files]
    workflow = CASAWorkflow(outdir, forecast_files,
                            container_sif=args.container_sif)
    workflow.create_pegasus_properties(hosted_site_catalog=args.hosted_site_catalog)
    workflow.create_transformation_catalog(exec_site_name=args.execution_site_name)
    workflow.create_replica_catalog()
    workflow.create_workflow()
    workflow.write()

    print(f"Workflow written to {workflow.dagfile}")
    print(
        f"Plan and submit: pegasus-plan --conf {workflow.propsfile} "
        f"--dir {os.path.join(outdir, 'submit')} "
        f"-s {args.execution_site_name} -o local --submit {workflow.dagfile}"
    )


if __name__ == "__main__":
    main()
