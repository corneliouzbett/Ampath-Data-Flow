import os
import re
import sys

def search_obs_value(obs, concept):
    return re.findall('## !!'+ str(concept) + '=(.+?)!! ##',obs)

obs= '!!1834=7850!! ## !!1915=Patient is awaiting imaging results, then start darcabazine 250 ml every 21 days.!! ## !!1915=Patient is HIV positive since 2016,no presents with pains on the plantar surface and pelvic pain.!! ## !!5096=2013-07-29!! ## !!5219=1068!! ## !!6504=7189!! ## !!6509=6511!! ## !!6575=1107!! ## !!6605=1065!! ## !!7191=MELANOMA OF THE RIGHT HEEL.!! ## !!8723=5622!!'
bett = search_obs_value(obs, '1915')

print(bett)