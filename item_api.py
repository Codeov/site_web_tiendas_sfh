from flask import Flask,request,render_template,url_for,redirect,flash,make_response,jsonify
from config import config
from flask_login import LoginManager,login_user,logout_user,login_required,current_user
import pandas as pd
import io
import time

from model.modelUser import modelUser
from model.entidad.user import User as user_form
from model.modelPollos import modelPollos
from model.modelVentaCero import modelVentaCero
from model.modelProcValRebate import modelProcValRebate
##inicio
app=Flask(__name__)
app.config.from_object(config['development'])
login_manager_app = LoginManager(app)

@login_manager_app.user_loader
def load_user(id):
    return modelUser.get_by_id(id)

@app.route('/',methods=['GET'])
def inicio():
 return redirect(url_for('login'))

@app.route('/login',methods=['POST','GET']) 
def login():
   if request.method=='POST':
      correo=request.form['email']
      contraseña=request.form['contraseña']
      user=user_form(0,correo,contraseña)
      loggen_user=modelUser.login(user)
      if loggen_user!=None:
         if loggen_user.password:
            login_user(loggen_user)
            return redirect(url_for('menu'))
         else:
            flash("Contraseña Invalida...")
            return render_template('login.html')
      else:
         flash("Usuario Invalido...")
         return render_template('login.html')
   else:
      return render_template('login.html')

@app.route('/menu')
@login_required
def menu():
    return render_template('menu.html')

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))

#pollo rostizado
@app.route('/pollosSemana', methods=['GET'])
@login_required
def pollosSemana():  
   pollosSemanaArray=modelPollos.filtro_semana()
   return jsonify({'pollosSemanaArray':pollosSemanaArray})

@app.route('/pollosLocal',methods=['GET'])
@login_required
def pollosLocal():
   pollosLocalArray=modelPollos.filtro_local()
   return jsonify({'pollosLocalArray':pollosLocalArray})

@app.route('/listarplan/<string:semana>/<string:local>',methods=['GET'])
@login_required
def listarplan(semana,local):
   planArray=modelPollos.listar_plan_horneado(semana,local)
   return jsonify({'planArray':planArray})

@app.route('/pollos',methods=["GET"])
@login_required
def pollos():
   alc_area='tiendas'
   alc_endpoint='pollo_rostizado'
   alc_user=int(current_user.id)
   val_rol_alc=modelUser.val_roles(alc_user,alc_area,alc_endpoint)
   if val_rol_alc:
    return render_template('pollos.html')
   else:
      flash("Lo siento, no tienes acceso a esta pagina...")
      return redirect(url_for('menu'))
#sistemas
@app.route('/costos',methods=['GET'])
@login_required
def costos():
   flash("Lo siento, no tienes acceso a esta pagina...")
   return redirect(url_for('menu'))
#ventacero
@app.route('/venta_cero',methods=["GET"])
@login_required
def venta_cero():
   alc_area='tiendas'
   alc_endpoint='venta_cero'
   alc_user=int(current_user.id)
   val_rol_alc=modelUser.val_roles(alc_user,alc_area,alc_endpoint)
   if val_rol_alc:
    return render_template('venta_cero.html')
   else:
      flash("Lo siento, no tienes acceso a esta pagina...")
      return redirect(url_for('menu'))

@app.route('/venta_cero_formato',methods=["GET"])
@login_required
def venta_cero_formato():
   formatoArray=modelVentaCero.filtro_formato()
   return jsonify({'formatoArray':formatoArray})

@app.route('/venta_cero_local/<string:formato>',methods=['GET'])
@login_required
def venta_cero_local(formato):
   localArray=modelVentaCero.filtro_local(formato)
   return jsonify({'localArray':localArray})

@app.route('/venta_cero_area',methods=["GET"])
@login_required
def venta_cero_area():
   areaArray=modelVentaCero.filtro_area()
   return jsonify({'areaArray':areaArray})

@app.route('/venta_cero_list/<string:local>/<string:area>',methods=['GET'])
@login_required
def venta_cero_list(local,area):
   ventaceroarray=modelVentaCero.select_venta_cero(local,area)
   return jsonify({'ventaceroarray':ventaceroarray})

@app.route('/venta_cero_group/<string:local>/<string:area>',methods=['GET'])
@login_required
def venta_cero_group(local,area):
   ventacerogrouparray=modelVentaCero.select_venta_cero_group(local,area)
   return jsonify({'ventacerogrouparray':ventacerogrouparray})

@app.route('/procesar_val_rebate',methods=['GET'])
@login_required
def procesar_val_rebate():
   alc_area='comercial'
   alc_endpoint='validacion_rebate'
   alc_user=int(current_user.id)
   val_rol_alc=modelUser.val_roles(alc_user,alc_area,alc_endpoint)
   if val_rol_alc:
    return render_template('proc_val_reb.html')
   else:
      flash("Lo siento, no tienes acceso a esta pagina...")
      return redirect(url_for('menu'))

@app.route('/procesar_val_rebate_input/<string:periodo>',methods=['GET','POST'])
@login_required
def procesar_val_rebate_input(periodo):
   format_fecha=periodo+"-01"
   print(format_fecha)
   flag_val_upd_reb_ac=modelProcValRebate.validar_upd_rebate_acumulado(format_fecha)
   if flag_val_upd_reb_ac==0:
      flag_data=modelProcValRebate.procesar_val_reb(periodo)
      return jsonify({'flag_data':flag_data})
   elif flag_val_upd_reb_ac==1:
      return jsonify({'flag_data':2})

@app.route('/actualizar_cierre_validacion/<string:fecha>',methods={'GET','POST'})
@login_required
def actualizar_cierre_validacion(fecha):
       format_fecha=fecha+"-01"
       print(format_fecha)
       flag_val_reb_ac=modelProcValRebate.validar_data_rebate_acumulado(format_fecha)
       if flag_val_reb_ac==True:
         flag_val_upd_reb_ac=modelProcValRebate.validar_upd_rebate_acumulado(format_fecha)
         if flag_val_upd_reb_ac==0:
            modelProcValRebate.actualizar_cierre_validacion(format_fecha)
            return jsonify({'flag_val_reb_ac_':1})
         else:
                return jsonify({'flag_val_reb_ac_':2})
       else:
             return jsonify({'flag_val_reb_ac_':0})
       
def status_401(error):
    return redirect(url_for('login'))

def status_404(error):
    return "<h1>Página no encontrada</h1>", 404

app.register_error_handler(401, status_401)
app.register_error_handler(404, status_404)
	
"""
if __name__=='__main__':
    app.config.from_object(config['development'])
    app.register_error_handler(401, status_401)
    app.register_error_handler(404, status_404)
    app.run()
#"""