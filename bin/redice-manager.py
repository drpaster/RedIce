import sys
from os import path
import argparse

try:
    HPATH = path.dirname(path.realpath(__file__))
except:
    HPATH = path.dirname(sys.path[0])

if (sys.version_info[0:-2] < ( 3,4,0 )):
    print('ERROR: Your python version is not supported by this APP version.')
    print('This version APP needs Python 3.4.0 or greater.')
    print('You are running: {0} version'.format(sys.version))
    sys.exit(1)

APP_ROOT = path.abspath(path.join(HPATH, path.pardir))
sys.path.insert(1, path.abspath(path.join(APP_ROOT, "redice")))

from redice import RedIce

def err_report(errors_dict):
    if errors_dict:
        print('\nOperation not performed, there are {0:d} errors:'.format(
            len(errors_dict)))
    i = 0
    for cur_e in errors_dict:
        i += 1
        print('{0:3s}{1:25s}: {2}'.format(
            '%d.'%(i), cur_e['name'], cur_e['desc']))
    sys.exit(1)


def router(args_obj):
    print(args_obj)
    RI = RedIce(args_obj.cmd)

    # Check RedIce Config on Valid
    if not RI.get_isconfig():
        err_report(RI.get_errors())

    # Route rules
    RoutesRI = {
        'root_reg': {
            'action': RI.reg_root,
            'args': []
            },
        'root_modify': {
            'action': RI.modify_root,
            'args': []
            },
        'root_list': {
            'action': RI.list_root,
            'args': []
            },
        'root_remove': {
            'action': RI.remove_root,
            'args': []
            },
        'root_addsentinel': {
            'action': RI.addsentinel_root,
            'args': []
            },
        'root_modifysentinel': {
            'action': RI.modifysentinel_root,
            'args': []
            },
        'root_removesentinel': {
            'action': RI.removesentinel_root,
            'args': []
            },
        'shard_add': {
            'action': RI.add_shard,
            'args': []
            }
    }
    route = '%s_%s'%(args_obj.cmd, args_obj.subcmd)

    if route == 'root_reg':
        RoutesRI[route]['args'] = [
            args_obj.name, args_obj.file, args_obj.default, args_obj.uuid]
    elif route == 'root_modify':
        RoutesRI[route]['args'] = [
            args_obj.toobj, args_obj.name, args_obj.default, args_obj.file]
    elif route == 'root_list':
        RoutesRI[route]['args'] = []
    elif route == 'root_remove':
        RoutesRI[route]['args'] = [
            args_obj.toobj, args_obj.with_file]
    elif route == 'root_addsentinel':
        RoutesRI[route]['args'] = [
            args_obj.name, args_obj.server_address, args_obj.uuid]
    elif route == 'root_modifysentinel':
        RoutesRI[route]['args'] = [
            args_obj.toobj, args_obj.name, args_obj.server_address]
    elif route == 'root_removesentinel':
        RoutesRI[route]['args'] = [args_obj.toobj]
    elif route == 'shard_add':
        RoutesRI[route]['args'] = [
            args_obj.name, args_obj.group, args_obj.root, args_obj.uuid]

    # Run command
    if not RoutesRI[route]['action'](*RoutesRI[route]['args']):
        err_report(RI.get_errors())

    #print(err_report(RI.get_errors()))
    print('OK')


def run():

    parser = argparse.ArgumentParser(
        prog='redice-manager',
        description='Process some integers.')

    parser.add_argument(
        '-v', '--version', dest='version', action='store_true',
        help='Current version')

    # parser_commands = argparse.ArgumentParser(
    #     prog='redice-manager',
    #     description='Process some integers.')

    # parser.add_argument(
    #     '-v', '--version', dest='version', action='store_true',
    #     help='Current version')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='List of commands')

    commands = {}

    # A list command parent level
    commands['list'] = subparsers.add_parser('list', help='')
    commands['check'] = subparsers.add_parser('check', help='')
    commands['root'] = subparsers.add_parser('root', help='')
    commands['map'] = subparsers.add_parser('map', help='')
    commands['shard'] = subparsers.add_parser('shard', help='')

    # Two level commands
    commands['list'].add_argument('-s', '--short', action='store_true', help='')
    commands['list'].add_argument(
        'obj', choices=['roots', 'maps', 'shards'])

    commands['check'].add_argument(
        'obj', choices=['roots', 'maps', 'shards'])

    # root command - Tree level commands
    commands['root_commands'] = commands['root'].add_subparsers(
        dest='subcmd', help='List of commands')

    commands['root_commands_reg'] = commands['root_commands'].add_parser(
        'reg', help='')
    commands['root_commands_reg'].add_argument(
        '--name', metavar='<rootname>',
        action='store', required=True,
        help='Set root name')
    commands['root_commands_reg'].add_argument(
        '--file', metavar='<filename>',
        action='store',
        help='Set root config file')
    commands['root_commands_reg'].add_argument(
        '--default',
        action='store_true',
        help='Set root as default')
    commands['root_commands_reg'].add_argument(
        '--uuid', metavar='<uuid>', action='store',
        help='Set a manual uuid for new root (optional)')

    commands['root_commands_modify'] = commands['root_commands'].add_parser(
        'modify', help='')
    commands['root_commands_modify'].add_argument(
        'toobj', metavar='<root-uuid>|<root-name>',
        action='store',
        help='')
    commands['root_commands_modify'].add_argument(
        '--name', metavar='<rootname>', action='store',
        help='New root name')
    commands['root_commands_modify'].add_argument(
        '--file', metavar='<filename>', action='store',
        help='New root config file')
    commands['root_commands_modify'].add_argument(
        '--default',
        action='store_true',
        help='Set root as default')


    commands['root_commands_remove'] = commands['root_commands'].add_parser(
        'remove', help='')
    commands['root_commands_remove'].add_argument(
        'toobj', metavar='<root-uuid>|<root-name>',
        action='store',
        help='Remove this root set')
    commands['root_commands_remove'].add_argument(
        '--with-file', action='store_true',
        help='Delete root config file')

    commands['root_commands_list'] = commands['root_commands'].add_parser(
        'list', help='')

    commands['root_commands_addsentinel'] = commands['root_commands'].add_parser(
        'addsentinel', help='')
    commands['root_commands_addsentinel'].add_argument(
        '--name', metavar='<sentinel-name>',
        action='store', required=True,
        help='Set sentinel server name')
    commands['root_commands_addsentinel'].add_argument(
        '--server-address', metavar='<host:port>',
        action='store', required=True,
        help='Set sentinel server address')
    commands['root_commands_addsentinel'].add_argument(
        '--uuid', metavar='<uuid>', action='store',
        help='Set a manual uuid for new sentinel server (optional)')

    commands['root_commands_modifysentinel'] = commands['root_commands'].add_parser(
        'modifysentinel', help='')
    commands['root_commands_modifysentinel'].add_argument(
        'toobj', metavar='<sentinel-uuid>|<sentinel-name>',
        action='store',
        help='')
    commands['root_commands_modifysentinel'].add_argument(
        '--name', metavar='<sentinel-name>',
        action='store',
        help='Set sentinel server name')
    commands['root_commands_modifysentinel'].add_argument(
        '--server-address', metavar='<host:port>',
        action='store',
        help='Set sentinel server address')

    commands['root_commands_removesentinel'] = commands['root_commands'].add_parser(
        'removesentinel', help='')
    commands['root_commands_removesentinel'].add_argument(
        'toobj', metavar='<sentinel-uuid>|<sentinel-name>',
        action='store',
        help='')



    # map command
    commands['map_commands'] = commands['map'].add_subparsers(
        dest='subcmd', help='')

    commands['map_commands_create'] = commands['map_commands'].add_parser(
        'create', help='')

    commands['map_commands_create'].add_argument(
        '--name', metavar='<map-name>',
        action='store', required=True,
        help='')
    commands['map_commands_create'].add_argument(
        '--blocks', metavar='<number>',
        type=int, action='store', required=True,
        help='Number - set total blocks count')
    # ToDo
    # commands['map_commands_create'].add_argument(
    #   '--use-shards', metavar='<group-uuid|group-name>',
    #    action='store', help='Set shards group to use for automatic allocation')
    # commands['map_commands_create'].add_argument(
    #   '--generate-shards', metavar='<number>',
    #    action='store', help='Set number slave servers for generated shard nodes')

    commands['map_commands_create'].add_argument(
        '--uuid', metavar='<uuid>', action='store',
        help='Set a manual uuid for new map (optional)')


    commands['map_commands_modify'] = commands['map_commands'].add_parser(
        'modify', help='')
    commands['map_commands_modify'].add_argument(
        'toobj', metavar='<map-uuid>|<map-name>',
        action='store',
        help='')
    commands['map_commands_modify'].add_argument(
        '--name', metavar='<map-name>',
        action='store', required=True,
        help='')

    commands['map_commands_list'] = commands['map_commands'].add_parser(
        'list', help='')
    commands['map_commands_list'].add_argument(
        'toobj', metavar='<map-uuid>|<map-name>',
        action='store',
        help='')
    commands['map_commands_list'].add_argument(
        '-s', '--short', action='store_true', help='')

    commands['map_commands_delete'] = commands['map_commands'].add_parser(
        'delete', help='')
    commands['map_commands_delete'].add_argument(
        'toobj', metavar='<map-uuid>|<map-name>',
        action='store',
        help='')


    # shard commands
    commands['shard_commands'] = commands['shard'].add_subparsers(
        dest='subcmd', help='')

    commands['shard_commands_add'] = commands['shard_commands'].add_parser(
        'add', help='')
    commands['shard_commands_add'].add_argument(
        '--name', metavar='<shard-name>',
        action='store', required=True,
        help='')
    commands['shard_commands_add'].add_argument(
        '--group', metavar='<group-name>',
        action='store',
        help='')
    commands['shard_commands_add'].add_argument(
        '--root',
        action='store_true',
        help='Add shard to current root (default)')
    commands['shard_commands_add'].add_argument(
        '--uuid', metavar='<shard-uuid>',
        action='store',
        help='Set a manual uuid for new shard (optional)')

    commands['shard_commands_modify'] = commands['shard_commands'].add_parser(
        'modify', help='')
    commands['shard_commands_modify'].add_argument(
        'toobj', metavar='<shard-uuid>|<shard-name>',
        action='store',
        help='')

    commands['shard_commands_modify'].add_argument(
        '--name', metavar='<shard-name>',
        action='store',
        help='')
    commands['shard_commands_modify'].add_argument(
        '--group', metavar='<group-uuid>|<group-name>',
        action='store',
        help='')

    commands['shard_commands_addserver'] = commands['shard_commands'].add_parser(
        'addserver', help='')
    commands['shard_commands_addserver'].add_argument(
        'toobj', metavar='<shard-uuid>|<shard-name>',
        action='store',
        help='')
    commands['shard_commands_addserver'].add_argument(
        '--name', metavar='<shard-name>',
        action='store', required=True,
        help='')
    commands['shard_commands_addserver'].add_argument(
        '--role', metavar='master|slave',
        choices=['master','slave'],  required=True,
        help='')
    commands['shard_commands_addserver'].add_argument(
        '--server-address', metavar='<host:port:db>',
        action='store', required=True,
        help='')
    commands['shard_commands_addserver'].add_argument(
        '--weight', metavar='<0-100>',
        type=int, choices=range(101),
        help='Set weight for server load')
    commands['shard_commands_addserver'].add_argument(
        '--uuid', metavar='<server-uuid>',
        action='store',
        help='Set a manual uuid for new server (optional)')

    commands['shard_commands_editserver'] = commands['shard_commands'].add_parser(
        'editserver', help='')

    commands['shard_commands_editserver'].add_argument(
        'toobj', metavar='<server-uuid>|<server-name>',
        action='store',
        help='')
    commands['shard_commands_editserver'].add_argument(
        '--shard', metavar='<shard-uuid>|<shard-name>',
        action='store',
        help='')
    commands['shard_commands_editserver'].add_argument(
        '--name', metavar='<server-name>',
        action='store',
        help='')
    commands['shard_commands_editserver'].add_argument(
        '--role', metavar='master|slave',
        choices=['master','slave'],
        help='')
    commands['shard_commands_editserver'].add_argument(
        '--server-address', metavar='<host:port:db>',
        action='store',
        help='')
    commands['shard_commands_editserver'].add_argument(
        '--weight', metavar='<0-100>',
        type=int, choices=range(101),
        help='Set weight for server load')

    commands['shard_commands_removeserver'] = commands['shard_commands'].add_parser(
        'removeserver', help='')
    commands['shard_commands_removeserver'].add_argument(
        'toobj', metavar='<server-uuid>|<server-name>',
        action='store',
        help='Remove server')


    commands['shard_commands_delete'] = commands['shard_commands'].add_parser(
        'delete', help='')
    commands['shard_commands_delete'].add_argument(
        'toobj', metavar='<shard-uuid>|<shard-name>',
        action='store',
        help='Delete shard')


    redice_args = parser.parse_args()
    #list_args = commands['list'].parse_args()

    #redice_commands = parser_commands.parse_args()

    #print("Args: ", redice_args)
    #print("List Args: ", list_args.short)


    return redice_args

if __name__ == '__main__':
    router(run())
