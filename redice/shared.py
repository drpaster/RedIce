import uuid
import re

class RedIceShared():
    """docstring for ."""
    def __init__(self, err_obj):
        self.errors_obj = err_obj

    def name_validate(self, name_str):
        results_list = []
        if name_str:
            results_list.append(
                bool(re.match('(^[a-zA-Z0-9]+([a-zA-Z\_0-9\.-@]*))$',
                              name_str)) and (
                                  len(name_str) >= 1 and len(name_str) <= 32))

        if not False in results_list:
            return True
        else:
            self.errors_obj.error_reg(
                'NameValidate',
                'Name %s is not valid'%(
                    name_str))
            return False

    def uuid4_validate(self, uuid_str, verbose=True):
        try:
            val = uuid.UUID(uuid_str, version=4)
        except ValueError:
            if verbose:
                self.errors_obj.error_reg(
                    'UUID4Validate',
                    'UUID %s is not valid'%(
                        uuid_str))
            return False
        else:
            return True

    def uuid4_or_name(self, str_val):
        if bool(str_val):
            if self.uuid4_validate(str_val, False):
                return 'uuid'
            elif self.name_validate(str_val):
                return 'name'
        return None
