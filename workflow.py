#!/usr/bin/env python3

import os
from Pegasus.api import (
    Workflow,
    File,
    Job,
    Namespace,
)

from datetime import datetime
from argparse import ArgumentParser


class CASAWorkflow(object):
    def __init__(self, outdir, forecast_fn):
        self.outdir = outdir
        self.forecast_fn = forecast_fn

    def generate_dax(self):
        "Generate a workflow"
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dax = Workflow("casa_nowcast-wf-%s" % ts)
        dax.add_metadata({"name": "CASA Nowcast"})

        # extract time
        string_end = self.forecast_fn[-1].find(".")
        file_time = self.forecast_fn[-1][string_end - 12 : string_end]
        file_time = file_time + "00"
        file_ymd = file_time[0:8]
        file_hms = file_time[8:14]

        # dax = ADAG("casa_nowcast-wf-%s" % file_time)
        # dax.metadata("name", "CASA Nowcast")

        # convert to individual minute files
        nowcast_split_job = Job("NowcastToWDSS2")
        nowcast_split_job.add_args(self.forecast_fn[-1])
        nowcast_split_job.add_args(".")
        forecast_file = File(self.forecast_fn[-1])
        nowcast_split_job.add_inputs(
            forecast_file,
        )
        for x in range(31):
            pr_file = File(f"PredictedReflectivity_{x}min_{file_ymd}-{file_hms}.nc")
            nowcast_split_job.add_outputs(
                pr_file, stage_out=True, register_replica=False
            )
        dax.add_jobs(nowcast_split_job)

        # run merged reflectivity threshold
        mrtconfigfile = File("mrtV2_config.txt")
        for x in range(31):
            pr_fn = f"PredictedReflectivity_{x}min_{file_ymd}-{file_hms}.nc"
            pr_file = File(pr_fn)
            pr_geojson = File(f"mrt_STORM_CASA_{x}_{file_ymd}-{file_hms}.geojson")
            mrt_job = Job("mrtV2")
            mrt_job.add_args("-c", mrtconfigfile)
            mrt_job.add_args(pr_fn)
            mrt_job.add_inputs(mrtconfigfile)
            mrt_job.add_inputs(pr_file)
            mrt_job.add_outputs(pr_geojson, stage_out=True, register_replica=False)
            mrt_job.add_profiles(Namespace.PEGASUS, "label", "label_" + str(x))
            dax.add_jobs(mrt_job)

        # generate image from max reflectivity
        colorscale = File("nexrad_ref.png")
        for x in range(31):
            pr_image_job = Job("merged_netcdf2png")
            pr_file = File("PredictedReflectivity_{x}min_{file_ymd}-{file_hms}.nc")
            pr_image = File(f"PredictedReflectivity_{x}min_{file_ymd}-{file_hms}.png")
            pr_image_job.add_args(
                "-c", colorscale, "-q 235 -z 0,75", "-o", pr_image, pr_file
            )
            pr_image_job.add_inputs(pr_file)
            pr_image_job.add_inputs(colorscale)
            pr_image_job.add_outputs(pr_image, stage_out=True, register_replica=False)
            pr_image_job.add_profiles(Namespace.PEGASUS, "label", "label_{x}")
            dax.add_jobs(pr_image_job)

        # Write the DAX file
        daxfile = os.path.join(self.outdir, dax.name + ".yml")
        dax.write(daxfile)
        print(daxfile)

    def generate_workflow(self):
        # Generate dax
        self.generate_dax()


if __name__ == "__main__":
    parser = ArgumentParser(description="CASA Nowcast Workflow")
    parser.add_argument(
        "-f",
        "--files",
        metavar="INPUT_FILE",
        type=str,
        nargs="+",
        help="Forecast Filename",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--outdir",
        metavar="OUTPUT_LOCATION",
        type=str,
        help="DAX Directory",
        required=True,
    )

    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)

    if not os.path.isdir(args.outdir):
        os.makedirs(outdir)

    workflow = CASAWorkflow(outdir, args.files)
    workflow.generate_workflow()
