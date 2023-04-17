import json
import re
import sys
import os


def parse_dependency(depends_on):
    dep = []
    if type(depends_on) is list:
        for i in depends_on:
            dep.append(i['ref'])
        return dep
    elif type(depends_on) is dict:
        dep.append(depends_on.get('ref'))
        return dep
    else:
        return dep


def replace_params(obj, env):
    cmd_lst = obj['command'].split(" ")
    cmd_values = obj['scriptArgument']
    arg = []
    for l in list(zip(obj['scriptArgument'], obj['command'].split(" ")[3:])):
        # arg.append(env['environment'][re.sub('[^A-Za-z0-9_]+', '', l[0])])
        arg.append(
            env.get('environment').get(re.sub('[^A-Za-z0-9\-_.]+', '', l[0]), re.sub('[^A-Za-z0-9\-_.]+', '', l[0])))
    cmd = ' '.join(cmd_lst[0:3] + arg).replace('${INPUT1_STAGING_DIR}', '/tmp')
    return cmd


def get_lineage(sf_conf):
    lineage = []
    for k in sf_conf.get('jobs'):
        dependency = sf_conf.get("jobs")[k]["dependsOn"]
        if not dependency:
            dependency = 'Nothing'
        else:
            dependency = " | ".join(dependency)
        lineage.append(f'{k} --> {dependency} \n')
    return lineage

def dp_to_sf_output(dp_file):
    with open(dp_file) as f:
        pipeline = json.load(f)
    objects = pipeline['objects']
    parameters = pipeline['parameters']
    values = pipeline.get('values', {})
    jobs = {}
    resources = {}
    env = {}
    http = {}
    for k in parameters:
        env[k['id']] = k['default']
    env = {'environment': env}

    for obj in objects:
        if obj.get('type') == 'EmrActivity':
            spk = obj["step"].split(',')
            try:
                spk.remove("command-runner.jar")
            except ValueError:
                pass
            jobs[obj['id']] = {'name': obj['id'], 'type': 'EmrActivity',
                               'runsOn': obj['runsOn']['ref'],
                               'step': spk,
                               'dependsOn': parse_dependency(obj.get('dependsOn'))}

        if obj.get('type') == 'ShellCommandActivity':
            jobs[obj['id']] = {'name': obj['id'], 'type': 'ShellCommandActivity',
                               'runsOn': obj['runsOn']['ref'],
                               'command': replace_params(obj, env),  # obj['command'],
                               'dependsOn': parse_dependency(obj.get('dependsOn'))}

        if obj.get('type') == 'Ec2Resource':
            resources[obj['id']] = obj
        if obj.get('type') == 'EmrCluster':
            resources[obj['id']] = obj
        if obj.get('type') == 'HttpProxy':
            http[obj['id']] = obj

    emr = {'emr': values}
    job = {'jobs': jobs}
    res = {'resources': resources}
    sched = {'schedule': {"scheduledAt": "11:00:00"}}
    http_proxy = {'http_proxy': http}
    sf_config = dict(env)
    sf_config.update(emr)
    sf_config.update(res)
    sf_config.update(http_proxy)
    sf_config.update(sched)
    sf_config.update(job)
    output_file=dp_file
    with open(output_file, "w") as f:
        json.dump(sf_config, f, indent=2)

