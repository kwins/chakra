//
// Created by kwins on 2021/5/27.
//

#ifndef CHAKRA_NOCOPY_H
#define CHAKRA_NOCOPY_H

namespace chakra::utils{

class UnCopyable {
public:
    UnCopyable() = default;

    UnCopyable(const UnCopyable &) = delete;

    UnCopyable &operator=(const UnCopyable &) = delete;
};


}



#endif //CHAKRA_NOCOPY_H
