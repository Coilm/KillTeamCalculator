import sqlite3
from typing import Optional

import KTSim
import polars as pl
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic.types import Json

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def help():
    return {"help": "help"}


@app.get("/simulation")
async def sim(op1: Json = Query(), op2: Json = Query(), cover: bool = False, obscured: bool = False, simnumber = 1000):
    attacker = KTSim.Operator(op1)
    defender = KTSim.Operator(op2)
    sim = KTSim.Simulation(attacker, defender, cover, obscured)
    print(sim.attacker)
    print(sim.defender)
    result = sim.run(int(simnumber))
    print(result)
    return {'result': result.tolist()}

@app.get("/operators")
async def get_operators():
    with sqlite3.connect("killteam2024.db") as con:
        df = pl.read_database(
            query="""SELECT DISTINCT operators.id, killteams.killteamname, operators.opname, operators.M, operators.APL, operators.SV, operators.W FROM operators
            JOIN killteams ON killteams.id = operators.killteam_id
            """,
            connection=con,
        )

    return df.to_dicts()

@app.get("/operators/KT/{killteamname}")
async def get_ktoperators(killteamname: str):
    with sqlite3.connect("killteam2024.db") as con:
        df = pl.read_database(
            query=f"""SELECT DISTINCT operators.id, killteams.killteamname, operators.opname, operators.M, operators.APL, operators.SV, operators.W FROM operators
            JOIN killteams ON killteams.id = operators.killteam_id
            WHERE killteams.killteamname = '{killteamname}'
            """,
            connection=con,
        )

    return df.to_dicts()

@app.get("/operators/profiles")
def get_operators_profiles():
    with sqlite3.connect("killteam2024.db") as con:

        df = pl.read_database(
            query="""SELECT DISTINCT operators.id, killteams.killteamname, operators.opname, operators.M, operators.APL, operators.SV, operators.W, 
            GROUP_CONCAT(DISTINCT weapons.wepname), GROUP_CONCAT(DISTINCT keywords.keyword) FROM operators
            JOIN killteams ON killteams.id = operators.killteam_id
            JOIN operators_weapons_specialrules ON operators_weapons_specialrules.op_id = operators.id
            JOIN weapons on operators_weapons_specialrules.wep_id = weapons.id
            JOIN operators_keywords ON operators.id = operators_keywords.left_id
            JOIN keywords ON keywords.id = operators_keywords.right_id
            GROUP BY operators.id
            """,
            connection=con,
        ).rename(
            {"GROUP_CONCAT(DISTINCT weapons.wepname)": "weapons", "GROUP_CONCAT(DISTINCT keywords.keyword)": "keywords"}
        )

    return df.to_dicts()


@app.get("/operators/{op_id}")
def get_operator(op_id: int, wep_id: Optional[int] = None):
    # res = cur.execute(f"SELECT * FROM operators where id = {op_id}")
    if wep_id is None:
        wep_id = "weapons.id"

    with sqlite3.connect("killteam2024.db") as con:
        df = pl.read_database(
            query=f"""SELECT operators.opname, operators.M, operators.APL, operators.SV, operators.W, weapons.wepname, weapons.BS, weapons.D, weapons.DCrit, specialrules.keyword FROM operators
            JOIN operators_weapons_specialrules ON operators_weapons_specialrules.op_id = {op_id}
            JOIN weapons ON operators_weapons_specialrules.wep_id = weapons.id
            JOIN specialrules ON operators_weapons_specialrules.sr_id = specialrules.id
            WHERE operators.id = {op_id} and weapons.id = {wep_id}
            """,
            connection=con,
        )

        keywords = pl.read_database(
            query=f"""SELECT keyword FROM keywords
            JOIN operators_keywords ON operators_keywords.right_id = keywords.id
            WHERE operators_keywords.left_id = {op_id}
            """,
            connection=con,
        )

    operator = df[0]["opname", "M", "APL", "SV", "W"].to_dicts()
    operator[0]["weapons"] = [
        {x[0][0]: {"BS": x[1]["BS"][0], "D": x[1]["D"][0], "DCrit": x[1]["DCrit"][0], "Keywords": x[1]["keyword"].to_list()}}
        for x in df.group_by("wepname").sort("wepname")
    ]
    operator[0]["keywords"] = keywords.to_series().to_list()

    # print(operator)
    return operator


@app.get("/operators/{op_id}/weapons/{wep_id}")
async def get_operator_wep(op_id: int, wep_id: int):
    operator = get_operator(op_id)
    operator[0]["weapons"] = operator[0]["weapons"][wep_id]

    return operator


@app.get("/killteams")
async def get_killteams():
    with sqlite3.connect("killteam2024.db") as con:
        df = pl.read_database(
            query="""SELECT killteamname FROM killteams
            """,
            connection=con,
        )

    return df.to_dicts()

@app.get("/killteam/name/{kt_name}/operator/name/{op_name}")
async def get_from_name_operator_wep(kt_name: str, op_name: str):
    with sqlite3.connect("killteam2024.db") as con:
        df = pl.read_database(
            query=f"""SELECT DISTINCT operators.opname, operators.SV, operators.W, weapons.wepname, weapons.A, weapons.BS, weapons.D, weapons.DCrit, weapons.weptype, specialrules.keyword FROM operators
            JOIN killteams ON killteams.id = operators.killteam_id
            JOIN operators_weapons_specialrules ON operators_weapons_specialrules.op_id = operators.id
            JOIN weapons ON operators_weapons_specialrules.wep_id = weapons.id
            JOIN specialrules ON operators_weapons_specialrules.sr_id = specialrules.id
            JOIN operators_keywords ON operators.id = operators_keywords.left_id
            JOIN keywords ON keywords.id = operators_keywords.right_id
            WHERE killteams.killteamname = '{kt_name}' and operators.opname = '{op_name}'
            """,
            connection=con,
        )

    return df.to_dicts()