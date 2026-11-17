def resolve_capture(reference,bindings):
    c=reference['capture'];key=(c['session'],c['operation'],c['field'])
    if key not in bindings:raise ValueError('MISSING_CAPTURE')
    return bindings[key]
