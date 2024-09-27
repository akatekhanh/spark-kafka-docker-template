from attrs import define, field


@define
class BaseCtx:
    logger: any = field()
    base_conf: dict = field()
    conf


@define
class SinkCtx(BaseCtx):
    pass