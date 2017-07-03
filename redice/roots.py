from os import path, makedirs, rename, remove
import uuid
from copy import deepcopy

import configparser

from errors import RedIceErrors
from shared import RedIceShared

class Roots():
    """docstring for ."""
    def __init__(self, cur_cmd):
        self.redice_errors = RedIceErrors()
        self.redice_shared = RedIceShared(self.redice_errors)

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

    def _read_redice_config(self):
        try:
            self.config.read(self.redice_conf_file)
        except Exception as e:
            self.redice_errors.error_reg('ReadRedIceConfig', e)
            return False
        else:
            for sel in self.config.sections():
                if self.redice_shared.uuid4_validate(sel):
                    self.registered_ids['roots']['uuid'].append(sel)
                    self.registered_ids['roots']['name'].append(
                        self.config[sel]['name'])
                    self.registered_ids['roots']['file'].append(
                        self.config[sel]['file'])
            if not self.redice_shared.uuid4_validate(self.config['main']['default_root']) and \
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
            self.redice_errors.error_reg('WriteRedIceConfig', e)
            return False
        else:
            return True

    def _read_root_config(self, root_uuid):
        try:
            self.config_root.read(self.config[root_uuid]['file'])
        except Exception as e:
            self.redice_errors.error_reg('ReadRootConfig', e)
            return False
        else:
            for sel in self.config_root.sections():
                if self.redice_shared.uuid4_validate(sel):
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
            self.redice_errors.error_reg('WriteRootConfig', e)
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
            self.redice_errors.error_reg(
                '%sIsHas'%(ids_type.upper()),
                '%s %s %s %s'%(
                    ids_type.upper(), ids_value, errmsg[expect], ids_class))
            return False


    def _conf_exists(self, conf_file):
        try:
            makedirs(path.dirname(conf_file), exist_ok=True)
        except Exception as e:
            self.redice_errors.error_reg('PathConfCheckCreate', e)
            return False

        if path.isfile(conf_file):
            return True
        else:
            try:
                open(conf_file, 'w').close()
            except Exception as e:
                self.redice_errors.error_reg('FileConfCheckCreate', e)
                return False
        if path.isfile(conf_file):
            return True
        else:
            self.redice_errors.error_reg(
                'ConfCheckAndCreate',
                'File %s is not created'%(
                    conf_file))
            return False


    def _get_uuid_by_name(self, config_obj, name):
        for sel in config_obj.sections():
            if self.redice_shared.uuid4_validate(sel):
                if config_obj[sel]['name'] == name:
                    return sel
        self.redice_errors.error_reg(
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

        ids_type = self.redice_shared.uuid4_or_name(ids_value)

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

    def get_errors(self):
        return self.redice_errors.get_errors()

    def get_isconfig(self):
        return self.isconfig

    def get_default_root_name(self):
        return self.config[self.config['main']['default_root']]['name']

    def reg_root(self, root_name, root_file=None, set_default=False, root_uuid=None):
        results_list = []

        if root_file is None:
            root_file=path.join(
                self.redice_conf_path,
                'roots',
                '%s%s'%(root_name, self.root_conf_sfx))
        if root_uuid is None:
            root_uuid=str(uuid.uuid4())

        results_list.append(self.redice_shared.uuid4_validate(root_uuid))
        results_list.append(self._ids_ishas('roots', 'uuid', root_uuid))
        results_list.append(self.redice_shared.name_validate(root_name))
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
            self.redice_errors.error_reg(
                'ModifyRoot',
                'Not specified what to change in root: %s'%(
                    obj))
            return False

        if bool(root_name) and self.config[root_uuid]['name'] != root_name:
            results_list.append(self.redice_shared.name_validate(root_name))
            results_list.append(self._ids_ishas('roots', 'name', root_name))
        else:
            self.redice_errors.error_reg(
                'ModifyRoot',
                'New root name: %s alredy exists'%(
                    root_name))
            results_list.append(False)

        if not False in results_list:
            if not self._ids_ishas('roots', 'file', root_file):
                return False

            if bool(root_file) and self.config[root_uuid]['file'] != root_file:
                self._conf_exists(self.config[root_uuid]['file'])
                try:
                    rename(self.config[root_uuid]['file'], root_file)
                    self.config[root_uuid]['file'] = root_file
                except Exception as e:
                    self.redice_errors.error_reg('RenameRootFile', e)
                    return False

            if bool(root_name):
                self.config[root_uuid]['name'] = root_name

            if set_default:
                self.config['main']['default_root'] = root_uuid

            self._conf_exists(root_file)
            return self._write_redice_config()
        else:
            return False

    def roots_list(self):
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
                    self.redice_errors.error_reg('DeleteRootConfig', e)
                    return False
                else:
                    return True
            return True
        else:
            return False

    def addsentinel_root(self, sentinel_name, sentinel_server, sentinel_uuid=None):
        self.redice_errors.errors_flush()
        results_list = []

        if sentinel_uuid is None:
            sentinel_uuid=str(uuid.uuid4())

        results_list.append(self.redice_shared.uuid4_validate(self.config['main']['default_root']))
        #print('FILE: ', self.redice_conf_file)
        results_list.append(self.redice_shared.uuid4_validate(sentinel_uuid))
        results_list.append(self._ids_ishas(
            'sentinels', 'uuid', sentinel_uuid))
        results_list.append(self.redice_shared.name_validate(sentinel_name))
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
            self.redice_errors.error_reg(
                'ModifySentinel',
                'Not specified what to change in sentinel: %s'%(
                    obj))
            return False

        if bool(sentinel_name) and self.config_root[sentinel_uuid]['name'] != sentinel_name:
            results_list.append(self.redice_shared.name_validate(sentinel_name))
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

    #API func: get sentinels as list for root sets as default
    def get_sentinels(self):
        sentinels_list = []
        for sentinel_uuid in self.registered_ids['sentinels']['uuid']:
            sentinels_list.append(
                {'uuid': sentinel_uuid,
                 'name': self.config_root[sentinel_uuid]['name'],
                 'server': self.config_root[sentinel_uuid]['server']}
            )
        return sentinels_list

    def listsentinels_root(self):
        print('Default root: {0:25s} {1}'.format(
            self.config[self.config['main']['default_root']]['name'],
            self.config['main']['default_root']
        ), '\n\nSentinel servers:')
        sentinels = self.get_sentinels()
        if sentinels:
            for sentinel in sentinels:
                print('{0:25s} {1}'.format(
                    sentinel['name'],
                    sentinel['server']))
        else:
            print('No registered sentinel server')
        return True
