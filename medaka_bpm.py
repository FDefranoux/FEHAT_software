#!/usr/bin/env python3
############################################################################################################
# Authors:
#   Sebastian Stricker, Uni Heidelberg, sebastian.stricker@stud.uni-heidelberg.de
#   Marcio Ferreira,    EMBL-EBI,       marcio@ebi.ac.uk
# Date: 08/2021
# License: GNU GENERAL PUBLIC LICENSE Version 3
###
# Main program file.
###
############################################################################################################
import gc

import logging
from pathlib import Path
import sys
import os
import argparse

import pandas as pd

import src.io_operations as io_operations
import src.setup as setup
import src.segment_heart as segment_heart
from src.job_utils import return_jobindex

# QC Analysis modules.
from qc_analysis.decision_tree.src import analysis as qc_analysis

# Load config
import configparser
curr_dir = Path(__file__).resolve().parent
config_path = curr_dir / 'config.ini'
curr_dir = Path(__file__).resolve().parent
config_path = curr_dir / 'config.ini'
config = configparser.ConfigParser()
config.read(config_path)

################################## GLOBAL VARIABLES ###########################
# LOGGER = logging.getLogger(__name__)

################################## ALGORITHM ##################################
# Analyse a range of wells
def analyse_directory(indir, args, channels, loops, wells=None):
    LOGGER.info("The analysis for each well can take one to several minutes")
    LOGGER.info("Running....please wait...")

    # Results for all wells
    results = pd.DataFrame()

    # Get trained model, if present. 
    trained_tree = io_operations.load_decision_tree()
    
    try:
        for well_frame_paths, video_metadata in io_operations.well_video_generator(indir, channels, loops):
            
            well_nr = int(video_metadata['well_id'][-3:])
            if wells is not None and well_nr not in wells:
                continue

            # Results of current well
            well_result = {}
            bpm = None
            fps = None
            qc_attributes = {}
            
            try:
                bpm, fps, qc_attributes = analyse_well(well_frame_paths, video_metadata, args)
                LOGGER.info(f"Reported BPM: {str(bpm)}")
                
                # Process data.
                # Important to rearrange the qc params in the same order used during training.
                # Easiest way to do that is to convert the qc_attributes to a dataframe and reorder the columns.
                # 'Stop frame' is not used during training.
                if trained_tree and bpm:
                    data = {k: v for k, v in qc_attributes.items() if k not in ["Stop frame"]}
                    data = pd.DataFrame.from_dict(qc_attributes, orient = "index").transpose()[qc_analysis.QC_FEATURES]
                    
                    # Get the qc parameter results evaluated by the decision tree as a dictionary.
                    qc_analysis_results = qc_analysis.evaluate(trained_tree, data)
                    well_result["qc_param_decision"] = qc_analysis_results[0]

                # qc_attributes may help in dev to improve the algorithm, but are unwanted in production.
                if True: #args.debug:
                    well_result.update(qc_attributes)

            except Exception as e:
                LOGGER.exception("Couldn't acquier BPM for well " + str(video_metadata['well_id'])
                                    + " in loop " +
                                    str(video_metadata['loop'])
                                    + " with channel " + str(video_metadata['channel']))
                well_result['error'] = "Error during processing. Check log files"

            finally:
                well_result['well_id']  = video_metadata['well_id']
                well_result['loop']     = video_metadata['loop']
                well_result['channel']  = video_metadata['channel']
                well_result['bpm']      = bpm
                well_result['fps']      = fps
                well_result['version']  = config['DEFAULT']['VERSION']
                
                well_result = pd.DataFrame(well_result, index=[0])
                results = pd.concat([results, well_result], ignore_index=True)

                gc.collect()

    except Exception as e:
        LOGGER.exception("Couldn't finish analysis")
        sys.exit()

    return results

# Run algorithm on a single well
def analyse_well(well_frame_paths, video_metadata, args):
    LOGGER.info("Analysing video - "
                + "Channel: " + str(video_metadata['channel'])
                + " Loop: " + str(video_metadata['loop'])
                + " Well: " + str(video_metadata['well_id'])
                )

    # TODO: I/O is very slow, 1 video ~500mb ~20s locally. Buffer video loading for single machine?
    # Load video
    video_metadata['timestamps'] = io_operations.extract_timestamps(well_frame_paths)

    video = io_operations.load_video(well_frame_paths, imread_flag=0)

    bpm, fps, qc_attributes = segment_heart.run(video, vars(args), video_metadata)

    return bpm, fps, qc_attributes


def main(indir, outdir, well_id, loop, channel, args, debug=False):
    ################################## STARTUP SETUP ##################################
    LOGGER.info("#######################")
    LOGGER.info("Program started with the following arguments: " + '\t'.join([str(indir), str(outdir), well_id, loop, channel]))
    analysis_id = '_'.join([well_id, loop, channel]) 
    
    ################################## MAIN PROGRAM START ##################################
    pattern = '*{}*{}*{}*.tif'.format(well_id, loop, channel)
    nr_files_for_analysis = list(indir.glob(pattern)) + list(indir.glob(pattern + 'f'))
    if len(nr_files_for_analysis) <= 0:
        LOGGER.error("Did not found any files corresponding")
        LOGGER.debug('Files correponding to following pattern {}: {}'.format(pattern, len(nr_files_for_analysis)))
        raise Exception('No File Found') 
    else:
        LOGGER.info(f'Found {len(nr_files_for_analysis)} for the analysis')
        LOGGER.debug('Files correponding to following pattern {}: {}'.format(pattern, len(nr_files_for_analysis)))
        well_nr = int(well_id[-3:])
        results = analyse_directory(indir, args, [channel], [loop], wells=[well_nr])

        ################################## OUTPUT ##################################
        io_operations.write_to_spreadsheet(Path(outdir / "results"), results, analysis_id)
        LOGGER.info("Finished analysis")
    LOGGER.info("#######################\n")


if __name__ == '__main__':
    LOGGER = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(
        description='Automated heart rate analysis of Medaka embryo videos')
    
    # General analysis arguments
    parser.add_argument('-i', '--indir',
                        help='Input directory',
                        required=True)
    
    parser.add_argument('-o', '--outdir',
                        help='Output directory. Default=indir',
                        default='.',
                        required=False)
    
    parser.add_argument('-w', '--well_id',
                        help='Restrict analysis to one well',
                        default=None,   
                        required=False)
    
    parser.add_argument('-a', '--well_array',
                        help='Restrict analysis to targeted wells in case of array',
                        default='[1-96]',   
                        required=False)    

    parser.add_argument('-l', '--loops',
                        help='Restrict analysis to loop',
                        required=True)

    parser.add_argument('-c', '--channels',
                        help='Restrict analysis to channel',
                        required=True)

    parser.add_argument('-f', '--fps',
                        help='Frames per second',
                        default=0.0,
                        required=False,
                        type=float)
    
    parser.add_argument('-s', '--embryo_size', 
                        help='radius of embryo in pixels',
                        default=300,
                        required=False, 
                        type=int)

    parser.add_argument('--cluster',
                        type=str, 
                        choices=['lsf', 'slurm', False], 
                        default=False,
                        help='Run analysis on a cluster',
                        required=False)
    parser.add_argument('--email',
                        action="store_true",
                        help='Receive email for cluster notification',
                        required=False)

    parser.add_argument('-m', '--maxjobs',
                        action="store",
                        help='maxjobs on the cluster',
                        required=False)
    
    parser.add_argument('--debug',
                        action="store_true",
                        help='Additional debug output',
                        required=False)
        
    args = parser.parse_args()

    # Parse input arguments.
    args.indir = Path(args.indir)
    if args.outdir:
        args.outdir = Path(args.outdir)
    _, args = setup.process_arguments(args, is_cluster_node=False) 
    args.loops =  list(args.loops)[0]
    args.channels =list(args.channels)[0]
    
    if args.well_id:
        analysis_id = '_'.join([args.well_id, args.loops,  args.channels]) 
        setup.config_logger(os.path.join(args.outdir, 'log'), ("logfile_hrt_bpm_" + analysis_id + ".log"), args.debug)
        main(indir=Path(args.indir), outdir=Path(args.outdir), well_id=args.well_id, loop=args.loops, channel=args.channels, debug=args.debug, args=args)
        sys.exit(0)
    elif args.well_array:
        range_wells = args.well_array[1:-1].split('-')
        well_ls = ['WE000' + str(n) if n > 9 else 'WE0000' + str(n) for n in range(int(range_wells[0]), int(range_wells[1])+1)]
    else:
        raise 'Need at least one well to run on'
    
    if args.cluster:
        job_index = return_jobindex()
    else:
        job_index = None
    
    #Run the jobs according to potential arrays        
    if job_index != None:
        well_id = well_ls[job_index -1]
        analysis_id = '_'.join([well_id, args.loops, args.channels]) 
        setup.config_logger(os.path.join(args.outdir, 'log'), ("logfile_hrt_bpm_" + analysis_id + ".log"), args.debug)
        main(indir=Path(args.indir), outdir=Path(args.outdir), well_id=well_id, loop=args.loops, channel=args.channels, debug=args.debug, args=args)
    
    elif len(well_ls) == 1:
        analysis_id = '_'.join([well_ls[0], args.loops, args.channels]) 
        setup.config_logger(os.path.join(args.outdir, 'log'), ("logfile_hrt_bpm_" + analysis_id + ".log"), args.debug)
        main(indir=Path(args.indir), outdir=Path(args.outdir), well_id=well_ls[0], loop=args.loops, channel=args.channels, debug=args.debug, args=args)
    
    else:
        #FIXME: Find a better way to run several wells at a time, multiprocesses ?
        for well_id in well_ls: 
            analysis_id = '_'.join([args.loops, args.channels]) 
            setup.config_logger(os.path.join(args.outdir, 'log'), ("logfile_hrt_bpm_" + analysis_id + ".log"), args.debug)
            try:
                main(indir=Path(args.indir), outdir=Path(args.outdir), well_id=well_id, loop=args.loops, channel=args.channels, debug=args.debug, args=args)
            except Exception:
                LOGGER.error('Error with well_id {}.'.format(well_id), exc_info=True)
                pass                