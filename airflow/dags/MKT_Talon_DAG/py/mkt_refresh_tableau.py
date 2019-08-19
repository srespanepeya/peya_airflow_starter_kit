### Se importa la libreria para tabajar con Tablau 
import tableauserverclient as TSC


def function_refresh_tableau_workbook(workbook_name)
    ###Se define la autenticación al servidor Tableau
    tableau_auth = TSC.TableauAuth('Talend.Tableau', '6DDwVnhJr7KF72eX', 'PedidosYa')

    ###Se crea la conexión al servidor de Tableau
    server = TSC.Server('http://tableau.deliveryhero.net',use_server_version=True)
    server.auth.sign_in(tableau_auth)

    ###Obtener el libro por nombre
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                    TSC.RequestOptions.Operator.Equals,
                                    workbook_name))
    matching_workbooks, pagination_item = server.workbooks.get(req_option)

    ###ID del libro que se va a ejecutar el extracto
    workbook_id = matching_workbooks[0].id


    ###Se ejecuta la actualizacion de los extractos del libro
    #Este proceso es asíncrono
    server.workbooks.refresh(workbook_id)

    ###Se libera la conexión
    server.auth.sign_out()
