import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
from flask import abort, render_template, Flask
import logging
import db

APP = Flask(__name__)


# Start page
@APP.route("/")
def index():
    stats = {}
    stats = db.execute(
        """
    SELECT * FROM
      (SELECT COUNT() n_occur FROM Ocorrencias)
    JOIN
      (SELECT COUNT() n_armas FROM Armas)
    JOIN
      (SELECT COUNT() n_areas FROM Areas)
    JOIN
      (SELECT COUNT() n_locais FROM Locais)
    JOIN
      (SELECT COUNT() n_vitimas FROM Vitimas)
    JOIN
      (SELECT COUNT() n_crimes FROM Crimes)
    """
    ).fetchone()
    logging.info(stats)
    return render_template("index.html", stats=stats)


# Ocorrencias
@APP.route("/ocorrencias/")
def list_ocorrencias():
    ocorrencias = db.execute(
        """
      SELECT occId, date_occ, date_rptd
      FROM ocorrencias
      ORDER BY date_occ desc
      """
    ).fetchall()
    return render_template("ocorrencias-list.html", ocorrencias=ocorrencias)


@APP.route("/ocorrencias/<int:id>/")
def get_ocorrencia(id):
    ocorrencias = db.execute(
        """
      SELECT occId, date_occ, date_rptd
      FROM Ocorrencias
      WHERE occId = ?
      """,
        [id],
    ).fetchone()

    if ocorrencias is None:
        abort(404, "Ocorrencia com id {} não existe.".format(id))

    crimes = db.execute(
        """
      SELECT crimeId, crimes.desc_crime
      FROM ocorrencias NATURAL JOIN occ_crime NATURAL JOIN crimes
      WHERE occId = ?
      ORDER BY crimeId
      """,
        [id],
    ).fetchone()

    vitimas = db.execute(
        """
      SELECT vitimaId, idade, sexo, descendencia
      FROM Ocorrencias NATURAL JOIN vitimas
      WHERE occId = ?
      ORDER BY vitimaId
      """,
        [id],
    ).fetchone()

    areas = db.execute(
        """
      SELECT areaId, nome
      FROM Ocorrencias NATURAL JOIN Locais NATURAL JOIN Areas
      WHERE occId = ?
      ORDER BY areaId
      """,
        [id],
    ).fetchone()

    locais = db.execute(
        """
      SELECT localId, desc_local, nome as area, coordenadas, morada
      FROM Ocorrencias NATURAL JOIN Locais NATURAL JOIN Areas
      WHERE occId = ?
      ORDER BY localId
      """,
        [id],
    ).fetchone()

    armas = db.execute(
        """
      SELECT armaId
      FROM Ocorrencias NATURAL JOIN Armas
      WHERE occId = ?
      ORDER BY armaId
      """,
        [id],
    ).fetchone()

    return render_template(
        "ocorrencia.html",
        ocorrencias=ocorrencias,
        crimes=crimes,
        vitimas=vitimas,
        areas=areas,
        locais=locais,
        armas=armas,
    )


# Locais
@APP.route("/locais/")
def list_locais():
    locais = db.execute(
        """
      SELECT localId, coordenadas, morada, desc_local, areaId
      FROM Locais NATURAL JOIN Areas
      ORDER BY desc_local
    """
    ).fetchall()
    return render_template("local-list.html", locais=locais)


@APP.route("/locais/<int:id>/")
def view_ocorrencias_by_local(id):
    local = db.execute(
        """
    SELECT localId, coordenadas, morada, desc_local, areaId
    FROM Locais
    WHERE localId = ?
    """,
        [id],
    ).fetchone()

    if local is None:
        abort(404, "Local id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT occId
    FROM Ocorrencias NATURAL JOIN Locais NATURAL JOIN Areas
    WHERE localId = ?
    ORDER BY localId
    """,
        [id],
    ).fetchall()

    return render_template("local.html", local=local, ocorrencias=ocorrencias)


@APP.route("/locais/search/<expr>/")
def search_local_by_desc(expr):
    search = {"expr": expr}
    expr = "%" + expr + "%"
    local = db.execute(
        """
        SELECT localId, coordenadas, morada, desc_local
        FROM Locais
        WHERE desc_local like ?
        """,
            [expr],
    ).fetchall()

    return render_template("local-search.html", search=search, local=local)

@APP.route("/locais/search-morada/<expr>/")
def search_local_by_morada(expr):
    search = {"expr": expr}
    expr = "%" + expr + "%"
    local = db.execute(
        """
        SELECT localId, coordenadas, morada, desc_local
        FROM Locais
        WHERE morada like ?
        """,
            [expr],
    ).fetchall()

    return render_template("local-search-morada.html", search=search, local=local)


# Crimes
@APP.route("/crimes/")
def list_crimes():
    crimes = db.execute(
        """
      SELECT crimeId, desc_crime
      FROM Crimes
      ORDER BY crimeId
    """
    ).fetchall()
    return render_template("crime-list.html", crimes=crimes)


@APP.route("/crimes/<int:id>/")
def view_ocorriencias_by_crime(id):
    crime = db.execute(
        """
    SELECT crimeId, desc_crime
    FROM Crimes
    WHERE crimeId = ?
    """,
        [id],
    ).fetchone()

    if crime is None:
        abort(404, "Crime id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT occID, date_occ, date_rptd
    FROM Ocorrencias NATURAL JOIN occ_crime NATURAL JOIN Crimes
    WHERE crimeId = ?
    ORDER BY date_occ
    """,
        [id],
    ).fetchall()

    return render_template("crime.html", crime=crime, ocorrencias=ocorrencias)


@APP.route("/crimes/search/<expr>/")
def search_crime(expr):
    search = {"expr": expr}
    expr = "%" + expr + "%"
    crime = db.execute(
        """ SELECT crimeId, desc_crime
        FROM Crimes
        WHERE desc_crime like ?
        """,
            [expr]
    ).fetchall()

    return render_template("crime-search.html", search=search, crime=crime)


# Vitimas
@APP.route("/vitimas/")
def list_vitimas():
    vitimas = db.execute(
        """
      SELECT vitimaId, idade, sexo, descendencia
      FROM Vitimas
      order by vitimaId
    """
    ).fetchall()
    return render_template("vitimas-list.html", vitimas=vitimas)


@APP.route("/vitimas/<int:id>/")
def view_ocorriencias_by_vitima(id):
    vitima = db.execute(
        """
    SELECT vitimaId, idade, sexo, descendencia
    FROM Vitimas
    WHERE vitimaId = ?
    """,
        [id],
    ).fetchone()

    if vitima is None:
        abort(404, "Vítima id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT occID, date_occ, date_rptd
    FROM Ocorrencias NATURAL JOIN Vitimas
    WHERE vitimaId = ?
    ORDER BY date_occ
    """,
        [id],
    ).fetchall()

    return render_template("vitima.html", vitima=vitima, ocorrencias=ocorrencias)


# Armas
@APP.route("/armas/")
def list_armas():
    armas = db.execute(
        """
      SELECT armaId, desc_arma
      FROM Armas
      ORDER BY armaId
    """
    ).fetchall()
    return render_template("armas-list.html", armas=armas)


@APP.route("/armas/<int:id>/")
def view_ocorriencias_by_arma(id):
    arma = db.execute(
        """
    SELECT armaId, desc_arma
    FROM  Armas
    WHERE armaId = ?
    ORDER BY armaId
    """,
        [id],
    ).fetchone()

    if arma is None:
        abort(404, "Arma id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT occId, date_occ, date_rptd
    FROM Ocorrencias NATURAL JOIN Armas
    WHERE armaId = ?
    ORDER BY date_occ
    """,
        [id],
    ).fetchall()

    return render_template("arma.html", arma=arma, ocorrencias=ocorrencias)


@APP.route("/armas/search/<expr>/")
def search_arma(expr):
    search = {"expr": expr}
    expr = "%"+expr+"%"
    arma = db.execute(
        """ SELECT armaId, desc_arma
        FROM Armas
        WHERE desc_arma like ?
        """,
            [expr],
    ).fetchall()

    return render_template("arma-search.html", search=search, arma=arma)


# Areas
@APP.route("/areas/")
def list_areas():
    areas = db.execute(
        """
      SELECT areaId, nome
      FROM Areas
      ORDER BY areaId
    """
    ).fetchall()
    return render_template("areas-list.html", areas=areas)


@APP.route("/areas/<int:id>/")
def view_ocorriencias_by_area(id):
    area = db.execute(
        """
    SELECT areaId, nome
    FROM Areas
    WHERE areaId = ?
    ORDER BY nome
    """,
        [id],
    ).fetchone()

    if area is None:
        abort(404, "Area id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT occId, date_occ, date_rptd
    FROM Ocorrencias NATURAL JOIN Locais NATURAL JOIN Areas
    WHERE areaId = ?
    ORDER BY date_occ
    """,
        [id],
    ).fetchall()

    return render_template("area.html", area=area, ocorrencias=ocorrencias)


@APP.route("/areas/search/<expr>/")
def search_area(expr):
    search = {"expr": expr}
    expr = "%"+expr+"%"
    area = db.execute(
        """ SELECT areaId, nome
        FROM Areas
        WHERE nome like ?
        ORDER BY nome
        """,
            [expr],
    ).fetchall()


    return render_template("area-search.html", search=search, area=area)
