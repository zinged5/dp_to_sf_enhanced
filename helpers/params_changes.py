import json
import re

#changes parameters in the exported json to the step functions format
def open_exported_file(v1_file):
    with open(v1_file) as f:
        file_output = json.load(f)
    return file_output

def get_params(v1_file):
    with open(v1_file) as f:
        params = json.load(f)
    return params

def make_params_changes(v1_file,dev_dp_file):
    output = v1_file
    target_emr = dict(get_params(dev_dp_file).get('emr'))
    target_env = dict(get_params(dev_dp_file).get('environment'))
    target_params = {**target_emr, **target_env}
    del target_env
    del target_emr
    for env_p in output.get('environment'):
        if env_p in target_params:
            output['environment'][env_p] = target_params.get(env_p)
    for env_p in output.get('emr'):
        if env_p in target_params:
            output['emr'][env_p] = target_params.get(env_p)
    find_specialchars=re.findall(r'\#{(.*?)\}', str(output))

    for p in find_specialchars:
        output = re.sub(f"#{{{p}}}", target_params.get(p), str(output))
    # added some additional changes for dev environment
    output=output.replace("-prod1","-dev1").replace("d1","d3").replace("p1","d3").replace("'",'"')
    j_output=json.loads(output)
    return j_output







