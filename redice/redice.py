from os import path, makedirs
import uuid
import re
from copy import deepcopy

import configparser

class RedIce():
    """docstring for ."""
    def __init__(self, cur_cmd):
        self.registered_errors = []

        self.redice_conf_path = path.join(
            path.expanduser('~'),
            '.redice')
        self.redice_conf_file = path.join(
            self.redice_conf_path,
            'redice.conf')
        self.cur_cmd = cur_cmd
        self.root_conf_sfx = '.conf'

        self.config = configparser.ConfigParser()
        x = {'uuids': [], 'names': [], 'ids': [], 'files': []}
        self.registered_ids = {
            'roots': deepcopy(x),
            'maps': deepcopy(x),
            'shards': deepcopy(x),
            'servers': deepcopy(x)
        }
        self.isconfig = self._read_redice_config()

    def _errors_reg(self, err_name, err_desc):
        self.registered_errors.append(
            {'name': err_name, 'desc': err_desc})

    def _read_redice_config(self):
        try:
            self.config.read(self.redice_conf_file)
        except Exception as e:
            self._errors_reg('ReadRedIceConfig', e)
            return False
        else:
            for sel in self.config.sections():
                if self._uuid4_validate(sel):
                    self.registered_ids['roots']['uuids'].append(sel)
                    self.registered_ids['roots']['names'].append(
                        self.config[sel]['name'])
                    self.registered_ids['roots']['files'].append(
                        self.config[sel]['file'])
            return True

    def _write_redice_onfig(self):
        try:
            with open(self.redice_conf_file, 'w') as configfile:
                self.config.write(configfile)
        except Exception as e:
            self._errors_reg('WriteRedIceConfig', e)
            return False
        else:
            return True

    def _ids_ishas(self, ids_class, ids_type, ids_value):
        if ids_value in self.registered_ids[ids_class][ids_type]:
            self._errors_reg(
                '%sIsHas'%(ids_type.upper()),
                '%s %s already exists in %s'%(
                    ids_type.upper(), ids_value, ids_class))
            return True
        else:
            return False

    def _uuid4_validate(self, uuid_str):
        try:
            val = uuid.UUID(uuid_str, version=4)
        except ValueError:
            self._errors_reg(
                'UUID4Validate',
                'UUID %s is not valid'%(
                    uuid_str))
            return False
        else:
            return True

    def _name_validate(self, name_str):
        if bool(
            re.match('(^[a-zA-Z0-9]+([a-zA-Z\_0-9\.-@]*))$', name_str)) and \
        (len(name_str) >= 1 and len(name_str) <= 128):
            return True
        else:
            self._errors_reg(
                'NameValidate',
                'Name %s is not valid'%(
                    name_str))
            return False

    def _root_conf_exists(self, conf_file):
        #print('Checked file: ', conf_file)
        makedirs(path.dirname(conf_file), exist_ok=True)

        if path.isfile(conf_file):
            return True
        else:
            self._errors_reg(
                'FileIsExists',
                'File %s is not created'%(
                    conf_file))
            return False

    def get_errors(self):
        return self.registered_errors

    def get_isconfig(self):
        return self.isconfig

    def reg_root(self, root_name, root_file=None, root_uuid=None):
        # print('New root, Name={0}, ConfFile={1}, UUID={2}'.format(
        #     root_name, root_file, root_uuid))

        results_list = []

        if root_file is None:
            root_file=path.join(
                self.redice_conf_path,
                'roots',
                '%s%s'%(root_name, self.root_conf_sfx))
        if root_uuid is None:
            root_uuid=str(uuid.uuid4())

        #print('FILE: ', self.redice_conf_file)
        results_list.append(self._uuid4_validate(root_uuid))
        results_list.append(not self._ids_ishas('roots', 'uuids', root_uuid))
        results_list.append(self._name_validate(root_name))
        results_list.append(not self._ids_ishas('roots', 'names', root_name))
        results_list.append(not self._ids_ishas('roots', 'files', root_file))


        #print('ERRORS: ', results_list)
        if not False in results_list:
            self.config[root_uuid] = {
                'name': root_name,
                'file': root_file
            }
            self._root_conf_exists(root_file)
            return self._write_redice_onfig()
        else:
            return False
