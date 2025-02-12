from typing import List

import requests
from sqlalchemy import ForeignKey, create_engine, select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)

req = requests.get("https://ktdash.app/api/faction.php?edition=kt24", timeout=5)
faction_dict = {r["factionid"]: r["factionname"] for r in req.json()}


class Base(DeclarativeBase):
    pass


"""
class OperatorsWeapons(Base):
    __tablename__ = "operators_weapons"
    left_id: Mapped[int] = mapped_column(ForeignKey("operators.id"), primary_key=True)
    right_id: Mapped[int] = mapped_column(ForeignKey("weapons.id"), primary_key=True)
    operators: Mapped["Operators"] = relationship(back_populates="weapons")
    weapons: Mapped["Weapons"] = relationship(back_populates="operators")

class WeaponsSpecialRules(Base):
    __tablename__ = "weapons_specialrules"
    left_id: Mapped[int] = mapped_column(ForeignKey("weapons.id"), primary_key=True)
    right_id: Mapped[int] = mapped_column(ForeignKey("specialrules.id"), primary_key=True)
    weapons: Mapped["Weapons"] = relationship(back_populates="specialrules")
    specialrules: Mapped["SpecialRules"] = relationship(back_populates="weapons")
"""


class OperatorsWeaponsSpecialRules(Base):
    __tablename__ = "operators_weapons_specialrules"
    op_id: Mapped[int] = mapped_column(ForeignKey("operators.id"), primary_key=True)
    wep_id: Mapped[int] = mapped_column(ForeignKey("weapons.id"), primary_key=True)
    sr_id: Mapped[int] = mapped_column(ForeignKey("specialrules.id"), primary_key=True)

    operators: Mapped["Operators"] = relationship(back_populates="weapons")
    weapons: Mapped["Weapons"] = relationship(back_populates="operators")
    specialrules: Mapped["SpecialRules"] = relationship(back_populates="weapons")


class OperatorsKeywords(Base):
    __tablename__ = "operators_keywords"
    left_id: Mapped[int] = mapped_column(ForeignKey("operators.id"), primary_key=True)
    right_id: Mapped[int] = mapped_column(ForeignKey("keywords.id"), primary_key=True)
    operators: Mapped["Operators"] = relationship(back_populates="keywords")
    keywords: Mapped["Keywords"] = relationship(back_populates="operators")


class KillTeams(Base):
    __tablename__ = "killteams"
    id: Mapped[int] = mapped_column(primary_key=True)
    faction: Mapped[str] = mapped_column()
    killteamname: Mapped[str] = mapped_column(unique=True)
    operators: Mapped[List["Operators"]] = relationship(back_populates="killteam")


class Operators(Base):
    __tablename__ = "operators"
    id: Mapped[int] = mapped_column(primary_key=True)
    killteam_id: Mapped[int] = mapped_column(ForeignKey("killteams.id"))
    killteam: Mapped["KillTeams"] = relationship(back_populates="operators")
    opname: Mapped[str] = mapped_column()
    M: Mapped[int] = mapped_column()
    APL: Mapped[int] = mapped_column()
    SV: Mapped[int] = mapped_column()
    W: Mapped[int] = mapped_column()

    weapons: Mapped[List["OperatorsWeaponsSpecialRules"]] = relationship(back_populates="operators")
    # specialrules: Mapped[List["OperatorsWeaponsSpecialRules"]] = relationship(back_populates="operators")
    keywords: Mapped[List["OperatorsKeywords"]] = relationship(back_populates="operators")


class Weapons(Base):
    __tablename__ = "weapons"
    id: Mapped[int] = mapped_column(primary_key=True)
    wepname: Mapped[str] = mapped_column()
    weptype: Mapped[str] = mapped_column()
    A: Mapped[int] = mapped_column()
    BS: Mapped[int] = mapped_column()
    D: Mapped[int] = mapped_column()
    DCrit: Mapped[int] = mapped_column()

    operators: Mapped[List["OperatorsWeaponsSpecialRules"]] = relationship(back_populates="weapons")
    specialrules: Mapped["OperatorsWeaponsSpecialRules"] = relationship(back_populates="weapons")

    @classmethod
    def get_or_create(session, self, **kwargs):
        exists = session.query(Weapons.id).filter_by(**kwargs).scalar() is not None
        if exists:
            return session.query(Weapons).filter_by(**kwargs).first()
        return self(**kwargs)


class Keywords(Base):
    __tablename__ = "keywords"
    id: Mapped[int] = mapped_column(primary_key=True)
    keyword: Mapped[str] = mapped_column()

    operators: Mapped[List["OperatorsKeywords"]] = relationship(back_populates="keywords")

    @classmethod
    def get_or_create(session, self, **kwargs):
        exists = session.query(Keywords.id).filter_by(**kwargs).scalar() is not None
        if exists:
            return session.query(Keywords).filter_by(**kwargs).first()
        return self(**kwargs)


class SpecialRules(Base):
    __tablename__ = "specialrules"
    id: Mapped[int] = mapped_column(primary_key=True)
    keyword: Mapped[str] = mapped_column()

    weapons: Mapped["OperatorsWeaponsSpecialRules"] = relationship(back_populates="specialrules")
    # operators: Mapped[List["OperatorsWeaponsSpecialRules"]] = relationship(back_populates="specialrules")

    @classmethod
    def get_or_create(self,session, **kwargs):
        exists = session.query(SpecialRules.id).filter_by(**kwargs).scalar() is not None
        if exists:
            return session.query(SpecialRules).filter_by(**kwargs).first()
        return self(**kwargs)


def write_KillTeamsTable(session):
    req = requests.get("https://ktdash.app/api/killteam.php?edition=kt24")
    for r in req.json():
        if faction_dict[r["factionid"]] in ["Special Teams", "Homebrew"]:
            continue
        newfaction = KillTeams(faction=faction_dict[r["factionid"]], killteamname=r["killteamname"])
        session.add(newfaction)
    session.commit()


def write_OperatorsWeaponsTable(session):
    req = requests.get("https://ktdash.app/api/killteam.php?edition=kt24")

    for killteam in req.json():
        if faction_dict[killteam["factionid"]] in ["Special Teams", "Homebrew"]:
            continue
        for fireteam in killteam["fireteams"]:
            factionid = fireteam["factionid"]
            if factionid in ["HBR", "NPO", "MALNPO"]:
                continue
            for op in fireteam["operatives"]:
                opname = op["opname"]
                m = op["M"].replace('"', "")
                apl = op["APL"]
                sv = op["SV"].replace("+", "")
                w = op["W"]
                ktid = session.execute(select(KillTeams).where(KillTeams.killteamname == killteam["killteamname"])).first()
                newOperator = Operators(killteam_id=ktid[0].id, opname=opname, M=m, APL=apl, SV=sv, W=w)
                session.add(newOperator)

                keywords = op["keywords"].split(",")
                for keyword in keywords:
                    keyword = keyword.strip()
                    newkeyword = Keywords.get_or_create(session, keyword=keyword)
                    if newkeyword not in session:
                        session.add(newkeyword)
                    session.commit()
                    Op_Keyword_rel = OperatorsKeywords(left_id=newOperator.id, right_id=newkeyword.id)
                    session.add(Op_Keyword_rel)

                for weapon in op["weapons"]:
                    for profile in weapon["profiles"]:
                        wepname = weapon["wepname"]
                        weptype = weapon["weptype"]
                        if profile["name"] != "":
                            wepname = wepname + " (" + profile["name"] + ")"
                        A = profile["A"]
                        BS = profile["BS"].replace("+", "")
                        try:
                            D, Dcrit = profile["D"].split("/")
                        except ValueError:
                            D = 0
                            Dcrit = 0
                        SR = profile["SR"].split(",")
                        newweapon = Weapons.get_or_create(session, wepname=wepname, weptype=weptype, A=A, BS=BS, D=D, DCrit=Dcrit)
                        if newweapon not in session:
                            session.add(newweapon)
                        session.commit()
                        # Op_Wep_rel = OperatorsWeapons(left_id=newOperator.id, right_id=newweapon.id)
                        # session.add(Op_Wep_rel)
                        # session.commit()
                        for sr in SR:
                            sr = sr.strip()
                            if sr == "":
                                continue
                            sr = fix_string(sr)

                            specialrules = SpecialRules.get_or_create(session, keyword=sr)
                            if specialrules not in session:
                                session.add(specialrules)
                            session.commit()
                            op_wep_sr_rel = OperatorsWeaponsSpecialRules(
                                op_id=newOperator.id, wep_id=newweapon.id, sr_id=specialrules.id
                            )
                            exists = (
                                session.query(OperatorsWeaponsSpecialRules.op_id)
                                .filter_by(op_id=newOperator.id, wep_id=newweapon.id, sr_id=specialrules.id)
                                .scalar()
                                is not None
                            )
                            if not exists:
                                session.add(op_wep_sr_rel)
                        session.commit()


def fix_string(sr):
    if sr == "*Anti-PSyker":
        sr = "*Anti-Psyker"
    if "Hvy" in sr:
        sr = sr.replace("Hvy", "Heavy")
    if "Heavy(" in sr:
        sr = sr.replace("Heavy(", "Heavy (")
    for i in range(0, 5):
        if f"Dev{i}" in sr:
            sr = sr.replace(f"Dev{i}", f"Dev {i}")
        if f"Blast{i}" in sr:
            sr = sr.replace(f"Blast{i}", f"Blast {i}")
        if f"Prc{i}" in sr:
            sr = sr.replace(f"Prc{i}", f"Prc {i}")
        if f"Rng{i}" in sr:
            sr = sr.replace(f"Rng{i}", f"Rng {i}")
        if f"Tor{i}" in sr:
            sr = sr.replace(f"Tor{i}", f"Tor {i}")
        if f"Lim{i}" in sr:
            sr = sr.replace(f"Lim{i}", f"Lim {i}")
    if "RepoOnly" in sr:
        sr = sr.replace("RepoOnly", "RepOnly")
    if "PrcCrit1" in sr:
        sr = sr.replace("PrcCrit1", "PrcCrit 1")
    if "PcrCrit1" in sr:
        sr = sr.replace("PcrCrit1", "PrcCrit 1")
    if "Piercing 1" in sr:
        sr = sr.replace("Piercing 1", "Prc 1")
    if sr == "Sat":
        sr = sr.replace("Sat", "Saturate")
    if sr == "Sil":
        sr = sr.replace("Sil", "Silent")
    if "Acc1" in sr:
        sr = sr.replace("Acc1", "Acc 1")
    if "Bal" in sr:
        sr = sr.replace("Bal", "Balanced")
    return sr


if __name__ == "__main__":
    engine = create_engine("sqlite:///killteam2024.db", echo=False)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    mainsession = Session()

    write_KillTeamsTable(mainsession)
    write_OperatorsWeaponsTable(mainsession)
    print("Writting done successfully.")
