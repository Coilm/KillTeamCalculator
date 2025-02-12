import datetime
import json

import numpy as np
import pandas as pd
import panel as pn
import param
import requests
from matplotlib import cm
from matplotlib.figure import Figure

placeholder = pn.pane.Placeholder("Hello")
pn.extension()

loading_gif = pn.pane.GIF('https://upload.wikimedia.org/wikipedia/commons/b/b1/Loading_icon.gif')

def update_weapon_list(attacker):
    kt_name = attacker['Kill Team']
    op_name = attacker['Operator']
    weps_req = requests.get(f"http://127.0.0.1:8000/killteam/name/{kt_name}/operator/name/{op_name}", timeout=5).json()
    result = {key: [d[key] for d in weps_req] for key in weps_req[0]}
    weps = pd.DataFrame(result).groupby('wepname').agg({'A': 'first', 'BS': 'first', 'D': 'first', 'DCrit': 'first', 'weptype': 'first', 'keyword': lambda tdf: ', '.join(tdf.unique().tolist())}).reset_index()
    weps = weps[weps['weptype'] == 'R']
    weapondf.object = weps[['wepname', 'A', 'BS', 'D', 'DCrit', 'keyword']]
    weapon_selector.options = weps['wepname'].to_list()
    # return weps[['wepname', 'BS', 'D', 'DCrit', 'keyword']]
    mpl_pane.object = None

def update_defender(defender):
    kt_name = defender['Kill Team']
    op_name = defender['Operator']
    weps_req = requests.get(f"http://127.0.0.1:8000/killteam/name/{kt_name}/operator/name/{op_name}", timeout=5).json()
    result = {key: [d[key] for d in weps_req] for key in weps_req[0]}
    weps = pd.DataFrame(result)
    weps = weps[weps['weptype'] == 'R']
    defender_info.object = weps[['opname', 'SV', 'W']].head(1)
    mpl_pane.object = None

def generate_simulation(attacker, weapons, defender, simnumber):
    fig = Figure(figsize=(8, 6))
    ax = fig.subplots()
    if len(weapons) == 0:
        return 0
    kt_name = attacker['Kill Team']

    op_name = attacker['Operator']
    wep_name = weapons
    defender_kt_name = defender['Kill Team']
    defender_op_name = defender['Operator']
    attacker_req = requests.get(f"http://127.0.0.1:8000/killteam/name/{kt_name}/operator/name/{op_name}", timeout=5).json()
    attacker_req = {key: [d[key] for d in attacker_req] for key in attacker_req[0]}
    defender_req = requests.get(f"http://127.0.0.1:8000/killteam/name/{defender_kt_name}/operator/name/{defender_op_name}", timeout=5).json()
    defender_req = {key: [d[key] for d in defender_req] for key in defender_req[0]}
    df_attacker = pd.DataFrame(attacker_req)
    df_defender = pd.DataFrame(defender_req)
    df_defender = df_defender.groupby('wepname').agg(
        {
            'opname': 'first', 
            'SV': 'first', 
            'W': 'first',
            'A': 'first', 
            'BS': 'first', 
            'D': 'first', 
            'DCrit': 'first', 
            'keyword': lambda tdf: tdf.unique().tolist()}
        ).reset_index()
    defender_json = json.dumps(df_defender.iloc[0].to_dict())

    for weapon in weapons:
        attacker_line = df_attacker[df_attacker['wepname'] == weapon].groupby('wepname').agg(
        {
            'opname': 'first', 
            'SV': 'first', 
            'W': 'first', 
            'A': 'first',
            'BS': 'first', 
            'D': 'first', 
            'DCrit': 'first', 
            'keyword': lambda tdf: tdf.unique().tolist()}
        ).reset_index()
        attacker_json = json.dumps(attacker_line.iloc[0].to_dict())
        r = {'op1': attacker_json, 'op2': defender_json, 'cover':False, 'obscured':False, 'simnumber':simnumber}
        simulation = requests.get('http://127.0.0.1:8000/simulation', params=r).json()
        counts, bins = np.histogram(simulation['result'])
        ax.stairs(counts/np.sum(counts) * 100, bins, label=weapon)
    
    ax.legend()
    mpl_pane.object = fig
    # mpl_pane.value = fig
        # print(simulation)

killteams = [x["killteamname"] for x in requests.get("http://127.0.0.1:8000/killteams", timeout=5).json()]
operators = {}

for kt in killteams:
    operators[kt] = [x["opname"] for x in requests.get(f"http://127.0.0.1:8000/operators/KT/{kt}", timeout=5).json()]

select_attacker = pn.widgets.NestedSelect(options=operators, levels=["Kill Team", "Operator"])
select_defender = pn.widgets.NestedSelect(options=operators, levels=["Kill Team", "Operator"])

weapondf = pn.pane.DataFrame(pd.DataFrame(), index=False)
weapon_selector = pn.widgets.CrossSelector(name='Weapons', value=[], options=[])
defender_info = pn.pane.DataFrame(pd.DataFrame(), index=False)
sim_input = pn.widgets.IntInput(name='Simulation Steps', value=10000, step=1000, start=10000, end=20000)

fig = Figure(figsize=(4, 3))
ax = fig.subplots()
mpl_pane = pn.pane.Matplotlib(fig, dpi=144)

pn.bind(update_weapon_list, attacker=select_attacker, watch=True)
pn.bind(update_defender, defender=select_defender, watch=True)
pn.bind(generate_simulation, attacker=select_attacker, weapons=weapon_selector, defender=select_defender, simnumber=sim_input, watch=True)

box = pn.GridBox(
    pn.Column('Attacker',
              select_attacker,
              weapondf,
              weapon_selector,
              styles=dict(background='WhiteSmoke')),
    pn.Column('Defender',
              select_defender,
              defender_info,
              styles=dict(background='WhiteSmoke')),

    ncols=2
    ).servable()

sim_input.servable()

mpl_pane.servable()
update_weapon_list(attacker=select_attacker.value)
update_defender(defender=select_defender.value)
generate_simulation(attacker=select_attacker.value, 
                    weapons=weapon_selector.value, 
                    defender=select_defender.value, 
                    simnumber=sim_input.value)


