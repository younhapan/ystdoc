# coding=utf8

class UserContact():
    phone = ''
    owner_id = ''
    created_time = None
    phone_location = ''
    created_at = None
    name = ''
    call_count = ''
    device_id = ''

class UserPhoneCall():
    phone = ''
    phone_location = ''
    location = ''
    owner_id = ''
    name = ''
    created_time = None
    calling_time = None
    # unit: second
    calling_duration = -1
    #  0:呼入, 1:呼出, 2:未接, 3:挂断
    type = -1
    device_id = ''


class UserShortMessage():
    phone = ''
    phone_location = ''
    source = ''
    owner_id = ''
    created_time = None

    sending_time = None
    name = ''
    content = ''
    # 0:呼入, 1:呼出, 2:发送失败或者发送中
    type = -1
    device_id = ''