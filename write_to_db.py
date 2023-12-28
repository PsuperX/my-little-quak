from typing import List, Literal, Tuple
import pandas as pd
import logging
from sqlite3 import Connection, connect


# Set this to False to not run extremely slow stuff
SLOW = True


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
        df["Vict Descent"] = df["Vict Descent"].replace(
            to_replace={
                "B": "Preto",
                "H": "Hispânico",
                "X": "Other",
                "W": "Branco",
                "A": "Asiático",
                "O": "Ocidental",
                "C": "Other",
                "F": "Other",
                "K": "Koreano",
                "I": "Indiano",
                "V": "Other",
                "Z": "Other",
                "J": "Other",
                "P": "Other",
                "G": "Other",
                "U": "Other",
                "D": "Other",
                "S": "Other",
                "L": "Other",
                "-": "Other",
            }
        )

        # Areas Table
        logging.info("Creating areas table...")
        to_sql(
            df[["AREA", "AREA NAME"]],
            "areas",
            con,
            renames={"AREA": "areaId", "AREA NAME": "nome"},
            primary_keys=["areaId"],
        )

        # Locais Table
        logging.info("Creating locais table...")
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
                "Premis Desc": "desc_local",
                "AREA": "areaId",
            },
            primary_keys=["localId"],
            foreign_keys=[("areaId", "areas(areaId)")],
        )

        # Armas Table
        logging.info("Creating armas table...")
        to_sql(
            df[["Weapon Used Cd", "Weapon Desc"]],
            "armas",
            con,
            renames={"Weapon Used Cd": "armaId", "Weapon Desc": "desc_arma"},
            primary_keys=["armaId"],
        )

        # Vitimas Table
        logging.info("Creating vitimas table...")
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
        logging.info("Creating crimes table...")
        crime_table = df[
            ["Crm Cd", "Crm Cd 1", "Crm Cd 2", "Crm Cd Desc", "DR_NO"]
        ].copy()
        temp = crime_table[["DR_NO", "Crm Cd 2"]].rename(
            {"Crm Cd 2": "Crm Cd"},
            axis="columns",
            copy=False,
            errors="raise",
        )
        temp.drop_duplicates(["Crm Cd"], inplace=True)
        crime_table = pd.concat([crime_table, temp], ignore_index=True)
        crime_table["Crm Cd Desc"] = crime_table["Crm Cd Desc"].fillna("Unknown")
        to_sql(
            crime_table[["Crm Cd", "Crm Cd Desc"]],
            "crimes",
            con,
            renames={"Crm Cd": "crimeId", "Crm Cd Desc": "desc_crime"},
            primary_keys=["crimeId"],
        )

        # Ocorrencias Table
        logging.info("Creating ocorrencias table...")


        if SLOW:
            df["DATE OCC"] = df.apply(
                lambda row: pd.to_datetime(
                    f"{row['DATE OCC'][:11]} {row['TIME OCC']:04d}",
                    format="%m/%d/%Y %H%M",
                ),
                axis=1,
            )
        to_sql(
            df[
                [
                    "DR_NO",
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
                "DR_NO": "occId",
                "Premis Cd": "localId",
                "Weapon Used Cd": "armaId",
                "DATE OCC": "date_occ",
                "Date Rptd": "date_rptd",
            },
            primary_keys=["occId"],
            foreign_keys=[
                ("vitimaId", "vitimas(vitimaId)"),
                ("localId", "locais(localId)"),
                ("armaId", "armas(armaId)"),
            ]
        )

        ## Occ-Crime
        logging.info("Creating occ-crime table...")
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS occ_crime")
        cur.execute(
            """
CREATE TABLE occ_crime (
    occId    INTEGER REFERENCES ocorrencias (occId),
    crimeId  INTEGER REFERENCES vitimas (vitimaId),
    PRIMARY KEY (occId, crimeId)
);
                    """
        )
        to_sql(
            crime_table[["Crm Cd", "DR_NO"]],
            "occ_crime",
            con,
            renames={
                "DR_NO": "occId",
                "Crm Cd": "crimeId",
            },
            primary_keys=["occId", "crimeId"],
            foreign_keys=[
                ("occId", "ocorrencias(occId)"),
                ("crimeId", "crimes(crimeId)"),
            ],
            if_exists="append",
        )

        logging.info("Done!")

        con.commit()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
