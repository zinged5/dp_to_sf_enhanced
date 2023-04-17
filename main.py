import json
from helpers import params_changes,dp_to_sf_v1,cfg_changes
import sys
import os


def generate_final_sf(dev_dp_file,prod_dp_file):
    dp_to_sf_v1.dp_to_sf_output(prod_dp_file)
    dp_to_sf_v1.dp_to_sf_output(dev_dp_file)
    source_output = cfg_changes.make_changes_to_cfg(prod_dp_file, dev_dp_file)
    output=params_changes.make_params_changes(source_output,dev_dp_file)
    output_file = f'3-sfn-input-{prod_dp_file}'
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

if __name__ == '__main__':
    dev_dp_file = sys.argv[1]
    prod_dp_file = sys.argv[2]
    generate_final_sf(dev_dp_file, prod_dp_file)




