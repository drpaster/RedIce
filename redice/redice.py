from os import path, makedirs, rename, remove
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
        self.config_root = configparser.ConfigParser()

        self.config['main'] = {
            'default_root': ''
        }

        redice_x = {'uuid': [], 'name': [], 'id': [], 'file': []}
        sentinel_x = {'uuid': [], 'name': [], 'server': []}
        self.registered_ids = {
            'roots': deepcopy(redice_x),
            'sentinels': deepcopy(sentinel_x)
        }

        self.isconfig = self._read_redice_config()
        self.isrootconfig = self._read_root_config(
            self.config['main']['default_root']
        )

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
                    self.registered_ids['roots']['uuid'].append(sel)
                    self.registered_ids['roots']['name'].append(
                        self.config[sel]['name'])
                    self.registered_ids['roots']['file'].append(
                        self.config[sel]['file'])
            if not self._uuid4_validate(self.config['main']['default_root']) and \
            len(self.registered_ids['roots']['uuid']) >= 1:
                self.config['main'] = self.registered_ids['roots']['uuid'][0]

                #elif sel == 'main':
                #     self
                # Todo if root conf file not found then create epty
            return True

    def _write_redice_config(self):
        try:
            with open(self.redice_conf_file, 'w') as configfile:
                self.config.write(configfile)
        except Exception as e:
            self._errors_reg('WriteRedIceConfig', e)
            return False
        else:
            return True

    def _read_root_config(self, root_uuid):
        try:
            self.config_root.read(self.config[root_uuid]['file'])
        except Exception as e:
            self._errors_reg('ReadRootConfig', e)
            return False
        else:
            for sel in self.config_root.sections():
                if self._uuid4_validate(sel):
                    self.registered_ids['sentinels']['uuid'].append(sel)
                    self.registered_ids['sentinels']['name'].append(
                        self.config_root[sel]['name'])
                    self.registered_ids['sentinels']['server'].append(
                        self.config_root[sel]['server'])
            return True

    def _write_root_config(self, root_uuid):
        try:
            with open(self.config[root_uuid]['file'], 'w') as configfile:
                self.config_root.write(configfile)
        except Exception as e:
            self._errors_reg('WriteRootConfig', e)
            return False
        else:
            return True

    def _ids_ishas(self, ids_class, ids_type, ids_value, expect=False):
        result_check = False

        errmsg = {
            False: 'already in use for other',
            True: 'not found for'
            }

        if ids_value in self.registered_ids[ids_class][ids_type]:
            result_check = True

        if expect is result_check:
            return True
        else:
            self._errors_reg(
                '%sIsHas'%(ids_type.upper()),
                '%s %s %s %s'%(
                    ids_type.upper(), ids_value, errmsg[expect], ids_class))
            return False

    def _uuid4_validate(self, uuid_str, verbose=True):
        try:
            val = uuid.UUID(uuid_str, version=4)
        except ValueError:
            if verbose:
                self._errors_reg(
                    'UUID4Validate',
                    'UUID %s is not valid'%(
                        uuid_str))
            return False
        else:
            return True

    def _name_validate(self, name_str):
        results_list = []
        if name_str:
            results_list.append(
                bool(re.match('(^[a-zA-Z0-9]+([a-zA-Z\_0-9\.-@]*))$',
                              name_str)) and (
                                  len(name_str) >= 1 and len(name_str) <= 32))

        if not False in results_list:
            return True
        else:
            self._errors_reg(
                'NameValidate',
                'Name %s is not valid'%(
                    name_str))
            return False

    def _conf_exists(self, conf_file):
        try:
            makedirs(path.dirname(conf_file), exist_ok=True)
        except Exception as e:
            self._errors_reg('PathConfCheckCreate', e)
            return False

        if path.isfile(conf_file):
            return True
        else:
            try:
                open(conf_file, 'w').close()
            except Exception as e:
                self._errors_reg('FileConfCheckCreate', e)
                return False
        if path.isfile(conf_file):
            return True
        else:
            self._errors_reg(
                'ConfCheckAndCreate',
                'File %s is not created'%(
                    conf_file))
            return False

    def _uuid4_or_name(self, str_val):
        if bool(str_val):
            if self._uuid4_validate(str_val, False):
                return 'uuid'
            elif self._name_validate(str_val):
                return 'name'
        return None

    def _get_uuid_by_name(self, config_obj, name):
        for sel in config_obj.sections():
            if self._uuid4_validate(sel):
                if config_obj[sel]['name'] == name:
                    return sel
        self._errors_reg(
            'UUIDByName',
            'Name %s not found'%(
                name))
        return None


    def _identify_uuid(self, ids_class, ids_value):
        if ids_class == 'roots':
            conf_obj = self.config
        elif ids_class == 'sentinels':
            conf_obj = self.config_root
        # ToDo elif|else 'shards, maps, servers'

        ids_type = self._uuid4_or_name(ids_value)

        if ids_type == 'uuid':
            uuid = ids_value
        elif ids_type == 'name':
            uuid = self._get_uuid_by_name(conf_obj, ids_value)
        else:
            return None

        if uuid:
            if self._ids_ishas(ids_class, 'uuid', uuid, True):
                return uuid
        else:
            return None

    def _errors_flush(self):
        self.registered_errors = []

    def get_errors(self):
        return self.registered_errors

    def get_isconfig(self):
        return self.isconfig

    def reg_root(self, root_name, root_file=None, set_default=False, root_uuid=None):
        results_list = []

        if root_file is None:
            root_file=path.join(
                self.redice_conf_path,
                'roots',
                '%s%s'%(root_name, self.root_conf_sfx))
        if root_uuid is None:
            root_uuid=str(uuid.uuid4())

        results_list.append(self._uuid4_validate(root_uuid))
        results_list.append(self._ids_ishas('roots', 'uuid', root_uuid))
        results_list.append(self._name_validate(root_name))
        results_list.append(self._ids_ishas('roots', 'name', root_name))
        if root_file:
            results_list.append(self._ids_ishas('roots', 'file', root_file))

        if not False in results_list:
            if set_default or len(self.registered_ids['roots']['uuid']) == 0:
                self.config['main']['default_root'] = root_uuid

            # create root conf file if not found
            # if no err then:
            self.config[root_uuid] = {
                'name': root_name,
                'file': root_file
            }
            return self._conf_exists(root_file) and self._write_redice_config()
            # end
        else:
            return False


    def modify_root(self, obj, root_name=None, set_default=False, root_file=None):

        results_list = []
        root_uuid = self._identify_uuid('roots', obj)

        if not root_uuid:
            return False

        # Check new values
        if not True in [bool(root_name), bool(root_file), set_default]:
            self._errors_reg(
                'ModifyRoot',
                'Not specified what to change in root: %s'%(
                    obj))
            return False

        if bool(root_name) and self.config[root_uuid]['name'] != root_name:
            results_list.append(self._name_validate(root_name))
            results_list.append(self._ids_ishas('roots', 'name', root_name))

        if not False in results_list:
            if not self._ids_ishas('roots', 'file', root_file):
                return False

            if bool(root_file) and self.config[root_uuid]['file'] != root_file:
                self._conf_exists(self.config[root_uuid]['file'])
                try:
                    rename(self.config[root_uuid]['file'], root_file)
                    self.config[root_uuid]['file'] = root_file
                except Exception as e:
                    self._errors_reg('RenameRootFile', e)
                    return False

            if bool(root_name):
                self.config[root_uuid]['name'] = root_name

            if set_default:
                self.config['main']['default_root'] = root_uuid

            self._conf_exists(root_file)
            return self._write_redice_config()
        else:
            return False


    def list_root(self):
        for uuid in self.registered_ids['roots']['uuid']:
            star = ''
            if uuid == self.config['main']['default_root']:
                star = '* '
            print('{0:2s}{1:25s} {2}'.format(
                star,
                self.config[uuid]['name'],
                uuid))
        return True

    def remove_root(self, obj, with_file):

        root_uuid = self._identify_uuid('roots', obj)
        if not root_uuid:
            return False

        root_file = self.config[root_uuid]['file']
        del self.config[root_uuid]
        if self._write_redice_config():
            if with_file:
                try:
                    remove(root_file)
                except Exception as e:
                    self._errors_reg('DeleteRootConfig', e)
                    return False
                else:
                    return True
            return True
        else:
            return False

    def addsentinel_root(self, sentinel_name, sentinel_server, sentinel_uuid=None):
        self._errors_flush()
        results_list = []

        if sentinel_uuid is None:
            sentinel_uuid=str(uuid.uuid4())

        results_list.append(self._uuid4_validate(self.config['main']['default_root']))
        #print('FILE: ', self.redice_conf_file)
        results_list.append(self._uuid4_validate(sentinel_uuid))
        results_list.append(self._ids_ishas(
            'sentinels', 'uuid', sentinel_uuid))
        results_list.append(self._name_validate(sentinel_name))
        results_list.append(self._ids_ishas(
            'sentinels', 'name', sentinel_name))
        if sentinel_server:
            results_list.append(self._ids_ishas(
                'sentinels', 'server', sentinel_server))

        print(self.registered_ids['sentinels']['name'])
        # if root_uuid in self.registered_ids['roots']['uuid']:

        print('ERRORS: ', results_list)
        if not False in results_list:
            self.config_root[sentinel_uuid] = {
                'name': sentinel_name,
                'server': sentinel_server
            }
            return self._write_root_config(self.config['main']['default_root'])
        else:
            return False


    def modifysentinel_root(self, obj, sentinel_name, sentinel_server):
        results_list = []
        sentinel_uuid = self._identify_uuid('sentinels', obj)

        if not sentinel_uuid:
            return False
        # Check new values
        if not True in [bool(sentinel_name), bool(sentinel_server)]:
            self._errors_reg(
                'ModifySentinel',
                'Not specified what to change in sentinel: %s'%(
                    obj))
            return False

        if bool(sentinel_name) and self.config_root[sentinel_uuid]['name'] != sentinel_name:
            results_list.append(self._name_validate(sentinel_name))
            results_list.append(self._ids_ishas('sentinels', 'name', sentinel_name))
            self.config_root[sentinel_uuid]['name'] = sentinel_name

        if bool(sentinel_server) and self.config_root[sentinel_uuid]['server'] != sentinel_server:
            results_list.append(self._ids_ishas('sentinels', 'server', sentinel_server))
            self.config_root[sentinel_uuid]['server'] = sentinel_server

        if not False in results_list:
            self._conf_exists(
                self.config[self.config['main']['default_root']]['file']
                )
            return self._write_root_config(self.config['main']['default_root'])
        else:
            return False



    def removesentinel_root(self, obj):
        sentinel_uuid = self._identify_uuid('sentinels', obj)
        if not sentinel_uuid:
            return False
        del self.config_root[sentinel_uuid]
        return self._write_root_config(self.config['main']['default_root'])

    def add_shard(self, name, group=None, uuid=None, root=False):
        print('New Shard: name={0}, group={1}, uuid={2}, root={3}, default_root={4}'.format(
            name, group, root, uuid, self.config['main']['default_root']
        ))
