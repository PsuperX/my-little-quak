from typing import List, Literal, Tuple
import pandas as pd
from sqlite3 import Connection, connect


def to_sql(
    df: pd.Series | pd.DataFrame,
    table_name: str,
    con: Connection,
    renames: dict[str, str],
    primary_keys: List[str] = [],
    foreign_keys: List[Tuple[str, str]] = [],
    if_exists: Literal["fail", "replace", "append"] = "replace",
):
    dtypes = {}
    dtypes[primary_keys[0]] = "INTEGER PRIMARY KEY"
    for k, r in foreign_keys:
        if k in dtypes:
            dtypes[k] += f" REFERENCES {r}"
        else:
            dtypes[k] = f"INTEGER REFERENCES {r}"

    df = df.rename(
        renames,
        axis="columns",
        copy=False,
        errors="raise",
    )
    df.dropna(subset=primary_keys, how="any", inplace=True)
    df.drop_duplicates(primary_keys, inplace=True)
    df.to_sql(
        name=table_name,
        dtype=dtypes,
        con=con,
        index=False,
        if_exists=if_exists,
    )


def main():
    df = pd.read_csv("Crime_Data_from_2020_to_Present.csv")

    with connect("data.db") as con:
        # Areas Table
        to_sql(
            df[["AREA", "AREA NAME"]],
            "areas",
            con,
            renames={"AREA": "areaId", "AREA NAME": "nome"},
            primary_keys=["areaId"],
        )

        # Locais Table
        t = df[["Premis Cd", "AREA", "LOCATION", "Premis Desc", "LAT", "LON"]]
        t = t.assign(coordenadas=t["LAT"].astype(str) + " " + t["LON"].astype(str))
        t.drop(["LAT", "LON"], axis=1, inplace=True)
        to_sql(
            t,
            "locais",
            con,
            renames={
                "Premis Cd": "localId",
                "LOCATION": "morada",
                "Premis Desc": "descricao",
                "AREA": "areaId",
            },
            primary_keys=["localId"],
            foreign_keys=[("areaId", "areas(areaId)")],
        )

        # Armas Table
        to_sql(
            df[["Weapon Used Cd", "Weapon Desc"]],
            "armas",
            con,
            renames={"Weapon Used Cd": "armaId", "Weapon Desc": "descricao"},
            primary_keys=["armaId"],
        )

        # Vitimas Table
        df = df.assign(vitimaId=t.index)
        t = df[["vitimaId", "Vict Age", "Vict Sex", "Vict Descent"]]
        to_sql(
            t,
            "vitimas",
            con,
            renames={
                "Vict Age": "idade",
                "Vict Sex": "sexo",
                "Vict Descent": "descendencia",
            },
            primary_keys=["vitimaId"],
        )

        # Crimes Table
        to_sql(
            df[["Crm Cd", "Crm Cd Desc"]],
            "crimes",
            con,
            renames={"Crm Cd": "crimeId", "Crm Cd Desc": "descricao"},
            primary_keys=["crimeId"],
        )

        # Ocorrencias Table
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS ocorrencias")
        cur.execute(
            """
CREATE TABLE ocorrencias (
    crimeId  INTEGER REFERENCES crimes (crimeId),
    vitimaId INTEGER REFERENCES vitimas (vitimaId),
    localId  INTEGER REFERENCES locais (localId),
    armaId   INTEGER REFERENCES armas (armaId),
    "data_occ" DATE,
    "data_rptd" DATE,
    PRIMARY KEY (crimeId, vitimaId, localId, armaId)
);
                    """
        )
        to_sql(
            df[
                [
                    "Crm Cd",
                    "vitimaId",
                    "Premis Cd",
                    "Weapon Used Cd",
                    "DATE OCC",
                    "Date Rptd",
                ]
            ],
            "ocorrencias",
            con,
            renames={
                "Crm Cd": "crimeId",
                "Premis Cd": "localId",
                "Weapon Used Cd": "armaId",
                "DATE OCC": "data_occ",
                "Date Rptd": "data_rptd",
            },
            primary_keys=["crimeId", "vitimaId", "localId"],
            foreign_keys=[
                ("crimeId", "crimes(crimeId)"),
                ("vitimaId", "vitimas(vitimaId)"),
                ("localId", "locais(localId)"),
                ("armaId", "armas(armaId)"),
            ],
            if_exists="append",
        )

        con.commit()


if __name__ == "__main__":
    main()
