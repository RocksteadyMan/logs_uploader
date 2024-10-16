from autocore.integration.jira_lib import Jira
from wowslib.constants.integrations import WOWsJira
from wowslib.configs import base_config

SYMBOLS = (u"абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ",
           u"abvgdeejzijklmnoprstufhzcss'y'euaABVGDEEJZIJKLMNOPRSTUFHZCSS'Y'EUA")
TRANSLATE_TABLE = {ord(a): ord(b) for a, b in zip(*SYMBOLS)}


def get_jira_task_summary(index):
    wows_jira = Jira(base_config.username, base_config.password, WOWsJira.server)
    return wows_jira.get_summary(index).translate(TRANSLATE_TABLE)


def clean_string(string):
    del_symbols = ('"', '*', r"'", '\\', '<', '|', ',', '>', '/', '?', ':', '{', '}', '#', '(', ')')
    for symbol in del_symbols:
        if symbol in string:
            string = string.replace(symbol, '_')
    string = ' '.join(string.split()).replace(' ', '_')
    return string