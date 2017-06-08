from os import path
import uuid
import re

class RedIce():
    """docstring for ."""
    def __init__(self, cur_cmd):
        self.cur_cmd = cur_cmd
        self.root_conf_file = 'root.conf'

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

    def reg_root(self, root_name, root_file=None, root_uuid=None):

        if root_file is None:
            root_file=path.join(
                path.expanduser('~'), '.redice', self.root_conf_file)
        if root_uuid is None:
            root_uuid=str(uuid.uuid4())

        #if _uuid4_validate(root_uuid):
        print('UUID is: ',
              self._uuid4_validate(root_uuid),
              self._uuid_ishas(root_uuid))
        print('Name is: ',
              self._name_validate(root_name),
              self._name_ishas(root_name))

        print('New root, Name={0}, ConfFile={1}, UUID={2}'.format(
            root_name, root_file, root_uuid))
