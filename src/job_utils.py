import os
import sys
import subprocess

MAIN_DIRECTORY = os.path.dirname(os.path.abspath(__file__)).replace('src', '')

def prepare_python_cmd(args, script_name):
    # processes to be dispatched
    arguments_variable = [
        ['--' + key, str(value)] for key, value in args.items() if value and value is not True]
    
    arguments_bool = ['--' + key for key,
                        value in args.items() if value is True]
    
    arguments = sum(arguments_variable, arguments_bool)

    # absolute filepath and sys.executeable for windows compatibility
    filename = os.path.join(MAIN_DIRECTORY, script_name) 
    python_cmd = [sys.executable, filename] + arguments
    
    return python_cmd

#FIXME: Tochange for a multiprocess?
def run_processes(cmd_list, max_subprocesses=5, log=sys.stdout):
    procs_list = []
    print("Processing " + str(max_subprocesses) + " subprocess at a time.", file=log)
    i = max_subprocesses
    for cmd in cmd_list:
        try:
            # f = open('/home/fannoux/Work/FEHATs/test_video_medaka_bpm_out_v1.5/blou.txt', 'w') 
            # p = subprocess.Popen(cmd, stdout=f, stderr=f)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        
            procs_list.append(p)

            experiment_name = cmd[cmd.index("--indir")+1]
            experiment_name = os.path.basename(os.path.normpath(experiment_name))
            print("Starting " + experiment_name, file=log)
            i -= 1

            if i == 0:
                for proc in procs_list:
                    proc.wait()
                print("Finished process set\n")
                i = max_subprocesses
        except Exception as err:
            print('Error with the process {}. The error message is {}'.format(cmd, err), file=log)
            pass
    for proc in procs_list:
        proc.wait()
    print("Finished all subprocesses.")

def run_cluster_and_getid(cmd):
    subp_out = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subp_stdout = subp_out.stdout.decode('utf-8')

    # Get jobId for consolidate command later
    job_id =[int(s) for s in subp_stdout.split() if s.isdigit()]
    if len(job_id) > 1:
        print('WARNING several digits that could correspond to a job_id have been detected in: ', subp_stdout)

    return job_id[0], subp_stdout

def lsf_command(script, walltime, jobname, memory, stdout, stderr, array=None, condition_job_ids=[]):
    if array:
        jobname += str(array) 
        stdout.replace('.log', '_%I.log')
        stderr.replace('.log', '_%I.log')
    if stdout == None:
        stdout = '/dev/null'
    if stderr == None:
        stderr = '/dev/null'
    lsf_cmd = f'bsub -W {walltime} -J {jobname} -M{memory} -R rusage[mem={memory}] -o {stdout} -e {stderr}'
    if len(condition_job_ids) > 0:
        condition = [("ended(" + str(s) + ")") for s in condition_job_ids]
        lsf_cmd += f" -w {'&&'.join(condition)}"
    lsf_cmd = lsf_cmd.split(' ')
    lsf_cmd += [f' \"{" ".join(script)}\"']
    return lsf_cmd

def slurm_command(script, walltime, jobname, memory, stdout, stderr, array=None, condition_job_ids=[]):
    if array:
        if stdout: stdout = stdout.replace('.log', '_%I.log')
        if stderr: stderr = stderr.replace('.log', '_%I.log')
    if stdout == None:
        stdout = '/dev/null'
    if stderr == None:
        stderr = '/dev/null'
    slurm_cmd = f'sbatch -t {walltime} --job-name={jobname} --mem={memory} -o {stdout} -e {stderr} --open-mode=truncate' #TODO: Remove the last part ?
    if array:
        slurm_cmd += f" --array={str(array)}"
    if len(condition_job_ids) > 0:
        slurm_cmd += f" -d afterany:{':'.join([str(s) for s in condition_job_ids])}"
    slurm_cmd = slurm_cmd.split(' ')
    slurm_cmd += ['--wrap', f'{" ".join(script)}']
    return slurm_cmd

def cluster_cmd(type, cluster_kwargs):
    if type == 'lsf':
        return lsf_command(**cluster_kwargs)
    elif type == 'slurm':
        return slurm_command(**cluster_kwargs)

def return_jobindex():
    if 'LSB_JOBINDEX' in os.environ:
        if os.environ['LSB_JOBINDEX'] == 0:
            job_index=None
        else:
            job_index = int(os.environ['LSB_JOBINDEX'])-1
    elif ('SLURM_ARRAY_TASK_ID' in os.environ) and ('SLURM_ARRAY_TASK_MIN' in os.environ):
        job_index = int(os.environ['SLURM_ARRAY_TASK_ID'])-int(os.environ['SLURM_ARRAY_TASK_MIN'])
    else:
        job_index=None
    return job_index