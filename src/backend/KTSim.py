from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

import numpy as np


def generate_dice(n, dice=4):
    # return np.random.randint(1,7, (n,dice))
    def populate(seed, vector, ix_start, ix_end):
        rng = np.random.default_rng(seed)
        vector[ix_start:ix_end] = rng.integers(1, 7, (ix_end - ix_start, dice), dtype=int)

    array = np.empty((n, dice), dtype=int)
    n_workers = cpu_count()
    with ThreadPool(n_workers) as pool:
        seed_seq = np.random.SeedSequence()
        seeds = seed_seq.spawn(n_workers)
        size = int(np.ceil(n / n_workers))
        args = [[seeds[i], array, i * size, (i + 1) * size] for i in range(n_workers)]
        if args[-1][3] > n:
            args[-1][3] = n
        pool.starmap(populate, args)
    return array


class Operator:
    def __init__(self, row):
        self.name = row["opname"] + " - " + row["wepname"]

        self.atk = row["A"]
        self.hit = row["BS"]
        self.dmg = row["D"]
        self.critdmg = row["DCrit"]
        self.save = row["SV"]
        self.wounds = row["W"]
        self.keywords = set(row["keyword"])

        self.lethal = 6
        self.piercing = 0
        self.devastating = 0
        self.accurate = 0

        for keyword in self.keywords:
            if "Lethal" in keyword:
                value = keyword.split(" ")[1][0]
                self.lethal = np.min([self.lethal, int(value)])
            if "Dev" in keyword:
                self.devastating = int(keyword.split("Dev")[1])
            if "Prc1" in keyword or "Prc2" in keyword:
                self.piercing = int(keyword.split("Prc")[1])
            if "Acc" in keyword:
                self.accurate = int(keyword.split("Acc")[1])


class Simulation:
    def __init__(self, offensive_profile, defensive_profile, cover=False, obscured=False):
        self.attacker = offensive_profile
        self.defender = defensive_profile
        self.cover = cover
        self.obscured = obscured
        if "Saturate" in self.attacker.keywords:
            self.cover = False

    def run(self, simstep=1):
        atk_success, atk_crit, _ = self.attack(simstep)
        def_success, def_crit, _ = self.defend(simstep, atk_crit)
        # results = np.vectorize(self.resulting_damage)(atk_success, atk_crit, def_success, def_crit)
        results = self.resulting_damage(atk_success, atk_crit, def_success, def_crit)
        return np.array(results)

    def attack(self, simstep=1):
        atk_rolls = generate_dice(simstep, self.attacker.atk)

        success = np.zeros(np.shape(atk_rolls)[0], dtype=int)
        crit = np.zeros(np.shape(atk_rolls)[0], dtype=int)
        fail = np.zeros(np.shape(atk_rolls)[0], dtype=int)

        for keyword in self.attacker.keywords:
            if "Acc" in keyword:
                success += keyword.split("Acc")[1]
                atk_rolls = atk_rolls[:, :-1]
                break

        # Check for reroll keyword
        # TODO: Implement the reroll mechanic to do the whole set of rolls instead of just one
        # for keyword in self.keywords:
        #    if keyword in ['Bal', 'Ceaseless', 'Relentless']:
        #        atk_rolls = self.reroll(atk_rolls, keyword)

        # Check for lethal
        crit += np.sum(atk_rolls >= self.attacker.lethal, axis=1)

        success += np.sum((atk_rolls >= self.attacker.hit) & (atk_rolls < self.attacker.lethal), axis=1)
        fail += self.attacker.atk - success - crit

        # Check for Severe:
        if "Severe" in self.attacker.keywords:
            success[np.where(np.logical_and((crit == 0), (success >= 1)))] -= 1
            crit[np.where(np.logical_and((crit == 0), (success >= 1)))] += 1

        # Check crit-based keyword
        if "Punishing" in self.attacker.keywords:
            fail[np.where(np.logical_and((crit > 0), (fail > 0)))] -= 1
            success[np.where(np.logical_and((crit > 0), (fail > 0)))] += 1
        if "Rending" in self.attacker.keywords:
            success[np.where(np.logical_and((crit > 0), (success > 0)))] -= 1
            crit[np.where(np.logical_and((crit > 0), (success > 0)))] += 1

        if self.obscured:
            success += crit
            crit = np.zeros(np.shape(crit))
            fail[np.where(success > 0)] += 1
            success[np.where(success > 0)] -= 1

        for roll, s, c, f in zip(atk_rolls, success, crit, fail):
            print(f"{roll} - success: {s}, crit: {c}, fail: {f}")
        return success, crit, fail

    def defend(self, simstep, atk_crit):
        success_def = np.zeros(simstep, dtype=int)
        crit_def = np.zeros(simstep, dtype=int)
        fail_def = np.zeros(simstep, dtype=int)

        dice_to_roll = np.zeros(simstep) + 3

        if "PrcCrit" in self.attacker.keywords:
            dice_to_roll[np.where(atk_crit > 0)] -= np.max(
                [self.attacker.piercing, int(self.attacker.keyword.split("PrcCrit")[1])]
            )
            dice_to_roll[np.where(atk_crit == 0)] -= self.attacker.piercing
        else:
            dice_to_roll -= self.attacker.piercing

        if self.cover:
            success_def[np.where(dice_to_roll > 0)] += 1
            dice_to_roll[np.where(dice_to_roll > 0)] -= 1

        vals, counts = np.unique(dice_to_roll, return_counts=True)

        defend_rolls = None
        for val, count in zip(vals.astype(int), counts.astype(int)):
            if count == 0:
                pass
            if defend_rolls is None:
                defend_rolls = generate_dice(count, val)

            else:
                defend_rolls = np.concatenate((defend_rolls, generate_dice(val, count)), axis=1, dtype=int)

            crit_def = np.sum(defend_rolls == 6, axis=1)
            success_def += np.sum((defend_rolls >= self.defender.save) & (defend_rolls < 6), axis=1)
            fail_def = 3 - success_def - crit_def

            return success_def, crit_def, fail_def

    def resulting_damage(self, atk_success, atk_crit, def_success, def_crit):
        if True:
            init = [[a, ac, d, dc] for a, ac, d, dc in zip(atk_success, atk_crit, def_success, def_crit)]

        damages = atk_crit * self.attacker.devastating
        MW = atk_crit * self.attacker.devastating
        # TODO: Rework this logic. It is not working properly. The conditions should be initialized before substracting the values, otherwise the evolve through the code.
        while np.sum(def_crit) > 0:
            def_crit_non_zero = def_crit > 0
            atk_crit_non_zero = atk_crit > 0
            atk_success_non_zero = atk_success > 0
            # LOGIC:
            # If there is no atk_crit, then remove a success
            log1 = def_crit_non_zero & (atk_crit == 0) & (atk_success_non_zero)
            # If there is an atk_crit and atk_success, and the dmg is more than the crit dmg, remove a success
            log2 = (
                def_crit_non_zero
                & (atk_crit_non_zero)
                & (atk_success_non_zero)
                & (self.attacker.dmg > self.attacker.critdmg)
            )

            atk_success[np.where(log1 | log2)] -= 1

            # If there is no atk, then remove a atk_crit
            log3 = def_crit_non_zero & (atk_crit_non_zero) & (atk_success == 0)
            # If there is an atk_crit and atk_success, and the dmg crit is more than the dmg, remove a crit
            log4 = (
                def_crit_non_zero
                & (atk_crit_non_zero)
                & (atk_success_non_zero)
                & (self.attacker.dmg <= self.attacker.critdmg)
            )

            atk_crit[np.where(log3 | log4)] -= 1

            # atk_crit[np.where((def_crit > 0) & (atk_crit > 0) & (atk_success > 0) & (self.attacker.dmg <= self.attacker.critdmg))] -= 1
            # atk_crit[np.where((def_crit > 0) & (atk_crit > 0) & (atk_success == 0))] -= 1

            # atk_success[np.where((def_crit > 0) & (atk_crit > 0) &(atk_success > 0) & (self.attacker.dmg > self.attacker.critdmg))] -= 1
            # atk_success[np.where((def_crit > 0) & (atk_crit <= 0) & (atk_success > 0))] -= 1

            def_crit[np.where(def_crit > 0)] -= 1

        if np.sum(atk_success) + np.sum(atk_crit) <= 0:
            return damages

        while np.sum(def_success) > 0:
            # LOGIC:
            # If there is at least two def success and an atk crit, and no atk success, remove an extra def success and an atk crit
            log0 = (def_success >= 2) & (atk_crit > 0) & (atk_success == 0)
            # If there is at least two def success and an atk crit, and atk success is less than 2 and the dmg is less than the crit dmg,
            # remove an extra def success and an atk crit
            log1 = (
                (def_success >= 2)
                & (atk_crit > 0)
                & (atk_success < 2)
                & (atk_success > 0)
                & (self.attacker.dmg <= self.attacker.critdmg)
            )
            # If there is at least two def success and an atk crit, and at least two atk success and the 2*dmg is less than the crit dmg,
            # remove an extra def success and an atk crit
            log2 = (
                (def_success >= 2)
                & (atk_crit > 0)
                & (atk_success >= 2)
                & (2 * self.attacker.dmg <= self.attacker.critdmg)
            )

            # If there is less than two success, remove an atk success
            log3 = (def_success < 2) & (def_success > 0) & (atk_success > 0)
            # If there is no atk crit, remove an atk success
            log4 = (atk_crit == 0) & (def_success > 0) & (atk_success > 0)
            # If there is at least two def success and an atk crit, and at least two atk success and the 2*dmg is more than the crit dmg,
            # remove two atk success
            log5 = (
                (def_success >= 2)
                & (atk_crit > 0)
                & (atk_success >= 2)
                & (2 * self.attacker.dmg > self.attacker.critdmg)
            )

            # If there is only one def success, remove an atk success
            log6 = (def_success == 1) & (atk_success > 0)

            atk_crit[np.where(log0 | log1 | log2)] -= 1
            atk_success[np.where(log3 | log4 | log6)] -= 1
            atk_success[np.where(log5)] -= 2

            def_success[np.where(log0 | log1 | log2 | log5)] -= 1
            def_success[np.where(def_success > 0)] -= 1

            # def_success[np.where((def_success >= 2) & (atk_crit > 0) & (atk_success < 2) & (atk_success > 0) & (self.attacker.dmg <= self.attacker.critdmg))] -= 1
            # atk_crit[np.where((def_success >= 2) & (atk_crit > 0) & (atk_success < 2) & (atk_success > 0) & (self.attacker.dmg <= self.attacker.critdmg))] -= 1

            # atk_success[np.where((def_success < 2) & (def_success > 0) & (atk_success > 0))] -= 1

            # def_success[np.where((def_success >= 2) & (atk_crit > 0) & (atk_success >= 2) & (2 * self.attacker.dmg <= self.attacker.critdmg))] -= 1
            # atk_crit[np.where((def_success >= 2) & (atk_crit > 0) & (atk_success >= 2) & (2 * self.attacker.dmg <= self.attacker.critdmg))] -= 1
            # atk_success[np.where((def_success >= 2) & (atk_crit > 0) & (atk_success >= 2) & (2 * self.attacker.dmg > self.attacker.critdmg))] -= 1

            # def_success[np.where(def_success > 0)] -= 1

        damages += atk_success * self.attacker.dmg + atk_crit * self.attacker.critdmg

        if False:
            for i, d, atk, atk_c, mw in zip(init, damages, atk_success, atk_crit, MW):
                print(i, d)
                print(f"{atk}*{self.attacker.dmg} + {atk_c}*{self.attacker.critdmg} + (MW: {mw}) = {d}")

        return damages
