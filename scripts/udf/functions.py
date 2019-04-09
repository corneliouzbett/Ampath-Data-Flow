import os
import re
import sys

def searchObsValue(obs, concept):
    try:
        found = re.search('## !!'+concept+'=(.+?)!! ##',obs)
    except AttributeError:
        found = 'null'
    return found