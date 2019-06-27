#!/usr/bin/env python

import sys
import os
import pwd
import time
from Pegasus.DAX3 import *
from datetime import datetime
from argparse import ArgumentParser

class CASAWorkflow(object):
    def __init__(self, outdir, forecast_fn):
        self.outdir = outdir
        self.forecast_fn = forecast_fn

    def generate_dax(self):
        "Generate a workflow"
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        dax = ADAG("casa_nowcast-wf-%s" % ts)
        dax.metadata("name", "CASA Nowcast")

        #extract time
        string_end = self.forecast_fn[-1].find(".")
        file_time = self.forecast_fn[-1][string_end-12:string_end]
        file_time = file_time + "00"
        file_ymd = file_time[0:8]
        file_hms = file_time[8:14]

        #convert to individual minute files
        nowcast_split_job = Job("NowcastToWDSS2")
        nowcast_split_job.addArguments(self.forecast_fn[-1]);
        nowcast_split_job.addArguments(".");
        forecast_file = File(self.forecast_fn[-1])
        nowcast_split_job.uses(forecast_file, link=Link.INPUT)
        for x in range(31):
            pr_file = File("PredictedReflectivity_"+ str(x) + "min_" + file_ymd + "-" + file_hms + ".nc")
            nowcast_split_job.uses(pr_file, link=Link.OUTPUT, transfer=True, register=False)          
        dax.addJob(nowcast_split_job)
        
        #run merged reflectivity threshold
        mrtconfigfile = File("mrtV2_config.txt")
        for x in range(31):
            pr_fn = "PredictedReflectivity_"+ str(x) + "min_" + file_ymd + "-" + file_hms + ".nc"
            pr_file = File(pr_fn)
            pr_geojson = File("mrt_STORM_CASA_" + str(x) + "_" + file_ymd + "-" + file_hms + ".geojson")
            mrt_job = Job("mrtV2")
            mrt_job.addArguments("-c", mrtconfigfile)
            mrt_job.addArguments(pr_fn)
            mrt_job.uses(mrtconfigfile, link=Link.INPUT)
            mrt_job.uses(pr_file, link=Link.INPUT)
            mrt_job.uses(pr_geojson, link=Link.OUTPUT, transfer=True, register=False)
            mrt_job.profile("pegasus", "label", "label_"+str(x))
            dax.addJob(mrt_job)
        
        # generate image from max reflectivity
        colorscale = File("nexrad_ref.png")
        for x in range(31):
            pr_image_job = Job("merged_netcdf2png")
            pr_file = File("PredictedReflectivity_"+ str(x) + "min_" + file_ymd + "-" + file_hms + ".nc")
            pr_image = File("PredictedReflectivity_"+ str(x) + "min_" + file_ymd + "-" + file_hms + ".png")
            pr_image_job.addArguments("-c", colorscale, "-q 235 -z 0,75", "-o", pr_image, pr_file)
            pr_image_job.uses(pr_file, link=Link.INPUT)
            pr_image_job.uses(colorscale, link=Link.INPUT)
            pr_image_job.uses(pr_image, link=Link.OUTPUT, transfer=True, register=False)
            pr_image_job.profile("pegasus", "label", "label_"+str(x))
            dax.addJob(pr_image_job)

        # Write the DAX file
        daxfile = os.path.join(self.outdir, dax.name+".dax")
        dax.writeXMLFile(daxfile)
        print daxfile

    def generate_workflow(self):
        # Generate dax
        self.generate_dax()

if __name__ == '__main__':
    parser = ArgumentParser(description="CASA Workflow")
    parser.add_argument("-f", "--files", metavar="INPUT_FILE", type=str, nargs="+", help="Forecast Filename", required=True)
    parser.add_argument("-o", "--outdir", metavar="OUTPUT_LOCATION", type=str, help="DAX Directory", required=True)

    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)
    
    if not os.path.isdir(args.outdir):
        os.makedirs(outdir)

    workflow = CASAWorkflow(outdir, args.files)
    workflow.generate_workflow()
