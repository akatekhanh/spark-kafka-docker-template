from attrs import define, field


@define
class BaseCtx:
    logger: any = field()
    base_conf: dict = field()


@define
class SinkCtx(BaseCtx):
    pass