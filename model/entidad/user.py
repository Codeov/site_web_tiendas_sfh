#from werkzeug.security import check_password_hash,generate_password_hash
from flask_login import UserMixin


class User(UserMixin):

    def __init__(self, id, username, password, fullname="") -> None:
        self.id = id
        self.username = username
        self.password = password
        self.fullname = fullname

    @classmethod
    #def check_password(self, hashed_password, password):
        #return check_password_hash(hashed_password, password)
    
    def check_password(self, hashed_password, password):
        if hashed_password==password:
            return True
        else:
            return False
        


#print(generate_password_hash('123', method='pbkdf2:sha1' , salt_length=8)) #python .\model\entidad\user.py