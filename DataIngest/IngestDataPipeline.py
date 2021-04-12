from azureml.core import Workspace, Datastore, Experiment
from azureml.core import Workspace, Dataset
from azureml.pipeline.core import PipelineParameter
from azureml.pipeline.core import Pipeline, PipelineRun
from azureml.pipeline.steps import PythonScriptStep

from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.runconfig import RunConfiguration
from azureml.core.compute import AmlCompute
from azureml.core.workspace import Workspace
from azureml.pipeline.core import PipelineParameter
from azureml.pipeline.core import Pipeline, PipelineRun,StepSequence
from azureml.pipeline.steps import PythonScriptStep
from azureml.core import Workspace, Datastore, Experiment

from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.runconfig import RunConfiguration
from azureml.core.compute import AmlCompute
from azureml.core.workspace import Workspace

import adal, uuid, time
import requests
import json
import pandas as pd


import datetime 
from datetime import timedelta
import pandas as pd
from azureml.core import Workspace, Datastore, Experiment,Dataset, Experiment, Run
from azureml.core.authentication import ServicePrincipalAuthentication




from azureml.data import OutputFileDatasetConfig
output_dir = OutputFileDatasetConfig(name="scores")
from azureml.pipeline.steps import ParallelRunStep, ParallelRunConfig

from azureml.core import Environment
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core import Dataset


subscription_id = 'bd04922c-a444-43dc-892f-74d5090f8a9a'
resource_group = 'mlplayarearg'
workspace_name = 'testdeployment'


workspace = Workspace(subscription_id, resource_group, workspace_name)

mydatastore = Datastore.get(workspace, 'billingdatablobstorage')

from azureml.data.datapath import DataPath, DataPathComputeBinding
from  azureml.pipeline.core.graph import PipelineParameter
data_path = DataPath(datastore=mydatastore, path_on_datastore='rawdata')
datapath1_pipeline_param = PipelineParameter(name="input_datapath", default_value=data_path)
datapath_input = (datapath1_pipeline_param, DataPathComputeBinding(mode='mount'))

string_pipeline_param = PipelineParameter(name="input_string", default_value='sample_string1')

compute_config = RunConfiguration()
compute_config.target = "cpu-cluster"

dependencies = CondaDependencies()
dependencies.add_pip_package("adal==0.4.7")
compute_config.environment.python.conda_dependencies = dependencies

StepToWriteDateFile = PythonScriptStep(
    name='StepToWriteDateFile',
    script_name="./DataIngest/StepToWriteDateFile.py",
    arguments=["--arg1", string_pipeline_param, "--arg2", datapath_input],
    inputs=[datapath_input],
    runconfig = compute_config ,
    #compute_target='manishautomlstuff', 
    source_directory='.')
print("StepToWriteDateFile created")

mydatastore = Datastore.get(workspace, 'billingdatablobstorage')
run = Run.get_context()
runId =  run.id
print(runId)

if runId[0:10] == "OfflineRun" :
    runId = "default"
else :
    runId = run.parent.id

dataset = Dataset.File.from_files(path = [(mydatastore, f"rawdata/daystoprocess/{runId}/*.csv")])

env = Environment(name="parallelenv")

env.from_conda_specification('parallelenv','./DataIngest/parallelenv.yml')


parallel_run_config = ParallelRunConfig(
   source_directory='.',
   entry_script='./DataIngest/parallelrunstep.py',
   mini_batch_size="1",
   error_threshold=30,
   output_action="append_row",
   environment=env,
   compute_target='cpu-cluster',
   append_row_file_name="my_outputs.txt",
   run_invocation_timeout=1200,
   node_count=1)

parallelrun_step = ParallelRunStep(
   name="parallelapicalls",
   parallel_run_config=parallel_run_config,
   arguments=["--arg1", string_pipeline_param],
   inputs=[dataset.as_named_input("inputds") ],
   output=output_dir
   #models=[ model ] #not needed as its only relevant in batch inferencing
   #arguments=[ ],
   #allow_reuse=True
)

print ('parallelrun_step created')

step_sequence = StepSequence(steps=[StepToWriteDateFile , parallelrun_step])
pipeline = Pipeline(workspace=workspace, steps=step_sequence)
print("pipeline with the 2 steps created")

pipeline.publish(name='New pipeline to pull usage', description='pull usage data in parallel', version="1.0", continue_on_step_failure=None)