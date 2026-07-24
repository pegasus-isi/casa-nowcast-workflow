#!/usr/bin/env python3
"""CASA Nowcast workflow generator (Pegasus 5).

Generates workflow.yml with the site, transformation, and replica catalogs
embedded, so it plans with a plain `pegasus-plan -s condorpool -o local
workflow.yml` — no external sites.xml/tc.txt/rc.txt and no --input-dir.

For each 30-minute nowcast forecast file (MERGE_DARTS_YYYYMMDDHHMM.nc):
  1. NowcastToWDSS2 splits it into 31 per-minute reflectivity files
  2. mrtV2 thresholds each into a storm GeoJSON
  3. merged_netcdf2png renders each into a PNG
"""

import os
import re
from argparse import ArgumentParser
from datetime import datetime, timezone

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
    ReplicaCatalog,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)


class CASAWorkflow(object):
    def __init__(self, outdir, forecast_files):
        self.outdir = outdir
        self.forecast_files = forecast_files

    def build_site_catalog(self):
        sc = SiteCatalog()

        base = os.getcwd()
        local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX)
        local.add_directories(
            Directory(
                Directory.SHARED_SCRATCH, os.path.join(base, "scratch")
            ).add_file_servers(
                FileServer("file://" + os.path.join(base, "scratch"), Operation.ALL)
            ),
            Directory(
                Directory.SHARED_STORAGE, os.path.join(base, "output")
            ).add_file_servers(
                FileServer("file://" + os.path.join(base, "output"), Operation.ALL)
            ),
        )

        condorpool = Site("condorpool", arch=Arch.X86_64, os_type=OS.LINUX)
        condorpool.add_condor_profile(universe="vanilla")
        condorpool.add_pegasus_profile(style="condor", data_configuration="condorio")

        sc.add_sites(local, condorpool)
        return sc

    def build_transformation_catalog(self):
        tc = TransformationCatalog()

        nowcast_container = Container(
            "nowcast_image",
            Container.DOCKER,
            image="docker:///pegasus/casa-nowcast",
            image_site="docker_hub",
        )
        tc.add_containers(nowcast_container)

        for name, pfn in (
            ("NowcastToWDSS2", "/opt/NowcastToWDSS2/NowcastToWDSS2"),
            ("mrtV2", "/opt/mrtV2/mrtV2"),
            ("merged_netcdf2png", "/opt/netcdf2png/merged_netcdf2png"),
        ):
            tc.add_transformations(
                Transformation(
                    name,
                    site="condorpool",
                    pfn=pfn,
                    is_stageable=False,
                    container=nowcast_container,
                    arch=Arch.X86_64,
                    os_type=OS.LINUX,
                )
            )
        return tc

    def build_replica_catalog(self):
        rc = ReplicaCatalog()
        input_dir = os.path.abspath("input")
        for aux in ("mrtV2_config.txt", "nexrad_ref.png"):
            rc.add_replica("local", aux, "file://" + os.path.join(input_dir, aux))
        for path in self.forecast_files:
            rc.add_replica("local", os.path.basename(path), "file://" + path)
        return rc

    def generate_workflow(self):
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        wf = Workflow("casa-nowcast-wf-%s" % ts)
        wf.add_metadata(name="CASA Nowcast")

        wf.add_site_catalog(self.build_site_catalog())
        wf.add_transformation_catalog(self.build_transformation_catalog())
        wf.add_replica_catalog(self.build_replica_catalog())

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

        wf_path = os.path.join(self.outdir, "workflow.yml")
        wf.write(wf_path)
        print(wf_path)


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


if __name__ == "__main__":
    parser = ArgumentParser(description="CASA Nowcast Workflow")
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
        help="Directory to write workflow.yml into (default: project root)",
    )

    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    forecast_files = [resolve_forecast_file(f) for f in args.files]
    CASAWorkflow(outdir, forecast_files).generate_workflow()
