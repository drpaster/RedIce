from os import path, makedirs
import uuid
import re
#import sys

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
        self.registered_roots = []
        self.isconfig = self._read_redice_config()


    def _read_redice_config(self):
        try:
            self.config.read(self.redice_conf_file)
        except Exception as e:
            self.registered_errors.append(
                {'f': 'ReadRedIceConfig', 'e': e})
            return False
        else:
            for sel in self.config.sections():
                if self._uuid4_validate(sel):
                    self.registered_roots.append(sel)
            return True

    def _write_redice_onfig(self):
        try:
            with open(self.redice_conf_file, 'w') as configfile:
                self.config.write(configfile)
        except Exception as e:
            self.registered_errors.append(
                {'f': 'WriteRedIceConfig', 'e': e})
            return False
        else:
            return True
        # finally:
        #     pass

    def _uuid_ishas(self, uuid_str):
        #ToDO
        return False

    def _name_ishas(self, uuid_str):
        #ToDO
        return False

    def _uuid4_validate(self, uuid_str):
        try:
            val = uuid.UUID(uuid_str, version=4)
        except ValueError:
            return False
        else:
            return True

    def _name_validate(self, name_str):
        if bool(
            re.match('(^[a-zA-Z0-9]+([a-zA-Z\_0-9\.-@]*))$', name_str)) and \
        (len(name_str) >= 1 and len(name_str) <= 128):
            return True
        else:
            return False

    def _root_conf_exists(self, root_conf_file):
        print('Checked file: ', root_conf_file)
        makedirs(path.dirname(root_conf_file), exist_ok=True)

        if path.isfile(root_conf_file):
            return True
        else:
            return False

    def get_errors(self):
        return self.registered_errors

    def get_isconfig(self):
        return self.isconfig

    def reg_root(self, root_name, root_file=None, root_uuid=None):

        if root_file is None:
            root_file=path.join(
                self.redice_conf_path,
                'roots',
                '%s%s'%(root_name, self.root_conf_sfx))
        if root_uuid is None:
            root_uuid=str(uuid.uuid4())


        self.config[root_uuid] = {
            'root_name': root_name,
            'root_file': root_file
        }

        #if _uuid4_validate(root_uuid):
        print('UUID is: ',
              self._uuid4_validate(root_uuid),
              self._uuid_ishas(root_uuid))
        print('Name is: ',
              self._name_validate(root_name),
              self._name_ishas(root_name))

        print('File conf check: ',
              self._root_conf_exists(root_file))

        print('New root, Name={0}, ConfFile={1}, UUID={2}'.format(
            root_name, root_file, root_uuid))

        return self._write_redice_onfig()
