import os, yaml, time, shutil


def generate_p_yaml(p_id, tap_config, target_config):
    from .p_app import PATH
    tap_id = tap_config["id"]
    target_id = target_config["id"]

    current_time = int(time.time() * 1e+5)
    config_dir = os.path.join(PATH.MEDIA_DIR, "{}".format(p_id))
    tap_filename = os.path.join(config_dir, "{}_{}.yaml".format(tap_id, current_time))
    target_filename = os.path.join(config_dir, "{}_{}.yaml".format(target_id, current_time))
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
    tap_ff = open(tap_filename, "w+")
    target_ff = open(target_filename, "w+")
    yaml.dump(tap_config, tap_ff, allow_unicode=True)
    yaml.dump(target_config, target_ff, allow_unicode=True)
    tap_ff.close()
    target_ff.close()
    return config_dir, tap_id, target_id


def run_tap_cli(config_dir, p_id, tap_id, target_id):
    from .p_app import PATH, p_config_db
    try:
        p_config_db.find_one_and_update({'_id': p_id}, {"$set": {"status": "RUNNING"}}, upsert=False)
        cmd = "/bin/bash -c \"{}/run_tap.sh {} {} {} {} {}; exit 0;\"".format(
                PATH.BASEDIR_APP,PATH.PIPELINEWISE_VENV, PATH.PIPELINEWISE_HOME, config_dir, tap_id, target_id)
        os_status = os.system(cmd)
        if os_status != 0:
            raise Exception("CLI pipelinewise fail.")
    except:
        pass

    p_config_db.find_one_and_update({'_id': p_id}, {"$set": {"status": "DONE"}}, upsert=False)
    if os.path.exists(config_dir):
        shutil.rmtree(config_dir)

# TODO implement pipelinewise event
