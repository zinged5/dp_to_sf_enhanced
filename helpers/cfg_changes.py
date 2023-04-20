import json
import re
import sys
import os
from helpers import params_changes as pc


def get_dp_dict(dev_dp_file):
    dp = pc.get_params(dev_dp_file)
    HttpProxy = {}
    Ec2Resource={}
    EmrResource={}
    Emr={}
    for i in dp.get('emr'):
        Emr[i]=dp['emr'][i]
    for i in dp.get('http_proxy'):
        HttpProxy['hostname'] = dp['http_proxy'][i]['hostname']
        HttpProxy['port'] = dp['http_proxy'][i]['port']
    for i in dp.get('resources'):
        if dp['resources'][i]['type']== 'Ec2Resource':
            Ec2Resource['role'] = dp['resources'][i]['role']
            Ec2Resource['resourceRole'] = dp['resources'][i]['resourceRole']
            Ec2Resource['ec2_resource'] = dp['resources'][i]['id']
        elif dp['resources'][i]['type']=='Ec2Resource':
            Ec2Resource['role'] = dp['resources'][i]['role']
            Ec2Resource['resourceRole'] = dp['resources'][i]['resourceRole']
            Ec2Resource['ec2_resource'] = dp['resources'][i]['id']
        elif dp['resources'][i]['type'] == 'EmrCluster':
            EmrResource['emr_resource']=dp['resources'][i]['id']

    dp_dict= {**Emr,**HttpProxy,**Ec2Resource,**EmrResource}
    del HttpProxy
    del Ec2Resource
    del EmrResource
    del Emr
    return dp_dict

def make_changes_to_cfg(v1_file,dev_dp_file):
    output=pc.open_exported_file(v1_file)
    dp_dict=get_dp_dict(dev_dp_file)
    output['environment']['myVar1_Partition'] = "bdp_insert_date"
    output['environment']['mySfn'] =f'3-sfn-main-{output["environment"].get("myPipeline")}'
    del output['environment']['myPipeline']
    if output['environment'].get('myLast_X_Days'):
        output['environment']['myLastXDay'] = output["environment"].get("myLast_X_Days")
        del output['environment']['myLast_X_Days']
    # emr changes
    if output['resources'].get(dp_dict.get('emr_resource')):
        # output['resources']['emr_resource'] = output['resources'].get(dp_dict.get('emr_resource'))
        output['resources']['emr_resource']['name'] = f'sf_{output["resources"]["emr_resource"]["name"]}'
        output['resources']['emr_resource']['StepConcurrencyLevel'] = 2
        output['resources']['emr_resource']['MinimumCapacityUnits'] = 1
        output['resources']['emr_resource']['MaximumCapacityUnits'] = 2
        output['resources']['emr_resource']['MaximumOnDemandCapacityUnits'] = 2
        output['resources']['emr_resource']['MaximumCoreCapacityUnits']= 2
        output['emr']['releaseLabel']=dp_dict.get('myEmrVersion')
        output['emr']['region']=dp_dict.get('myRegion')
        output['emr']['subnetId'] = dp_dict.get('mySubnetId')
        output['emr']['emrManagedMasterSecurityGroupId']=dp_dict.get('myMasterSecurityGroup')
        output['emr']['emrManagedSlaveSecurityGroupId']=dp_dict.get('mySlaveSecurityGroup')
        output['emr']['serviceAccessSecurityGroupId']=dp_dict.get('myServiceAccessSecurityGroup')
        output['emr']['serviceRole']=dp_dict.get('role')
        output['emr']['jobflowRole']=dp_dict.get('resourceRole')
        output['emr']['logUri']=f's3://{output["environment"].get("myBucket")}/stepfunctions/emr/logs/{output["environment"].get("mySfn")}/'
        output['emr']['http_proxy'] = dp_dict.get('hostname')
        output['emr']['http_port'] = dp_dict.get('port')
        output['schedule']= {"scheduledAt": "",
                                            "frequency": "",
                                            "day": ""}
        del output['resources']['emr_resource']
        del output['resources']['emr_resource']['id']
        del output['resources']['emr_resource']['type']
        del output['resources']['emr_resource']['releaseLabel']
        del output['resources']['emr_resource']['subnetId']
        del output['resources']['emr_resource']['emrManagedMasterSecurityGroupId']
        del output['resources']['emr_resource']['emrManagedSlaveSecurityGroupId']
        del output['resources']['emr_resource']['serviceAccessSecurityGroupId']
        del output['resources']['emr_resource']['region']
        del output['resources']['emr_resource']['applications']
        del output['resources']['emr_resource']['actionOnTaskFailure']
        del output['resources']['emr_resource']['httpProxy']
        del output['emr']['myEmrVersion']
        del output['emr']['myRegion']
        del output['emr']['mySubnetId']
        del output['emr']['myMasterSecurityGroup']
        del output['emr']['mySlaveSecurityGroup']
        del output['emr']['myServiceAccessSecurityGroup']
        del output['emr']['myMasterInstanceType']
        del output['emr']['myCoreInstanceType']
        del output['emr']['myInstanceCount']
    # ec2 changes
    # output['resources']['ec2_resource'] = output['resources'].get(dp_dict.get('ec2_resource'))
    # output['resources']['ec2_resource']['name'] = f'sf_{output["resources"]["ec2_resource"]["name"]}'
    # del output['resources']['ec2_resource']
    del output['resources']['ec2_resource']['id']
    del output['resources']['ec2_resource']['name']
    del output['resources']['ec2_resource']['schedule']
    del output['resources']['ec2_resource']['httpProxy']
    # outputs the json in order and sorts the 'jobs' in order.
    output = {'environment' :{'InputStagingDir':"/tmp/","myVar1_Partition":"bdp_insert_date",**output['environment'] },"emr":output.get('emr'),\
                      "resources":output['resources'],"schedule":output['schedule'],"jobs": dict(sorted(dict(output['jobs']).items()))}
    return output



