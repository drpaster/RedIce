class RedIceErrors():
    """docstring for ."""
    def __init__(self):
        self.registered_errors = []

    def errors_flush(self):
        self.registered_errors = []

    def get_errors(self):
        return self.registered_errors

    def error_reg(self, err_name, err_desc):
        self.registered_errors.append(
            {'name': err_name, 'desc': err_desc})
