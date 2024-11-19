class config:
    SECRET_KEY='Borico199719@1'
    DEBUG=True

class DevConfig(config):
    pass

class ProdConfig(config):
    DEBUG=False

config={
    'development': DevConfig,
    'prod': ProdConfig
}