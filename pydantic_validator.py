""" Pydantic issue https://github.com/pydantic/pydantic/issues/12085 """

import typing
import pydantic

print(pydantic.version.version_info())

COUNTER: int = 0


class Simple(pydantic.BaseModel):
    """ The most simple model, no attribute required (but fails in realistic situations as well)"""
    model_config = pydantic.ConfigDict(revalidate_instances='never')

    @pydantic.model_validator(mode='after')
    def not_much(self) -> typing.Self:
        """ Does nothing to self, keep global COUNTER to demonstrate we don't even have to use the model """
        global COUNTER
        COUNTER += 1
        return self


class Composed(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(revalidate_instances='never')
    simple: Simple


simple = Simple()
assert COUNTER == 1, f'model_validator Simple.not_much called: {COUNTER=}'  # passes
c = Composed(simple=simple)
assert COUNTER == 1, f'model_validator Simple.not_much called: {COUNTER=}'  # fails
