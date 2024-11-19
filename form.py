from wtforms import Form, BooleanField, StringField, PasswordField, validators,ValidationError

def validate_codigo(form,field):          
        try:
             int(field.data)
        except Exception as e:
             raise ValidationError('parametros incorrectos')

class activarLocCT2(Form):
    codigo_local_pmm = StringField('Codigo Local PMM', 
                                   [
                                    validators.Length(min=1, max=5),
                                    validators.data_required(),
                                    validate_codigo
                                   ]
                                    )
    
    