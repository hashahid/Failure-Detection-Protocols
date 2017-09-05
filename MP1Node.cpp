/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

// TODO - DOCUMENTATION!!!

const int MP1Node::numTargetMembers = 2;
const FailureDetectionProtocol MP1Node::protocol = SWIM;

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
    
    this->hasUpdatesToGive = false;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((std::queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();
    
    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }
    
    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }
    
    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TGOSSIP;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif
    
    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        
        free(msg);
    }
    
    return 1;
    
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    memberNode->bFailed = true;
    memberNode->inited = false;
    memberNode->inGroup = false;
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TGOSSIP;
    memberNode->timeOutCounter = -1;
    memberNode->memberList.clear();
    
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }
    
    // Check my messages
    checkMessages();
    
    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }
    
    // ...then jump in and share your responsibilites!
    nodeLoopOps();
    
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
    
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    bool messageProcessed = true;
    
    MessageHdr receivedMessageHeader;
    memcpy(&receivedMessageHeader, data, sizeof(MessageHdr));
    
    char *dataWithoutHeader = data + sizeof(MessageHdr);
    
    switch (receivedMessageHeader.msgType) {
        case JOINREQ:
            processJoinRequest(dataWithoutHeader);
            break;
        case JOINREP:
            memberNode->inGroup = true;
            updateMembershipList(dataWithoutHeader);
            break;
        case HEARTBEAT:
            updateMembershipList(dataWithoutHeader);
            break;
        case PULLREQUEST:
            processPullRequest(dataWithoutHeader);
            break;
        case PING:
            processPing(dataWithoutHeader);
            break;
        case ACK:
            processAck(dataWithoutHeader);
            break;
        case PINGTOSURROGATE:
            processPingToSurrogate(dataWithoutHeader);
            break;
        case PINGFROMSURROGATE:
            processPingFromSurrogate(dataWithoutHeader);
            break;
        case ACKTOSURROGATE:
            processAckToSurrogate(dataWithoutHeader);
            break;
        case ACKFROMSURROGATE:
            processAckFromSurrogate(dataWithoutHeader);
            break;
        default:
            messageProcessed = false;
            break;
    }
    
    return messageProcessed;
}

void MP1Node::processJoinRequest(char *data) {
    // get member list entry info from data
    int id;
    short port;
    long heartbeat;
    
    memcpy(&id, data, sizeof(int));
    size_t memOffset = sizeof(int);
    
    memcpy(&port, data + memOffset, sizeof(short));
    memOffset += sizeof(short);
    
    memcpy(&heartbeat, data + memOffset, sizeof(long));
    
    MessageHdr *msg = nullptr;
    size_t msgsize = createHealthyMembershipListMsg(&msg, JOINREP);
    
    MemberListEntry newEntry(id, port, heartbeat, memberNode->timeOutCounter);
    
    Address newEntryAddr = getMemberListEntryAddress(&newEntry);
    
    // send JOINREP message to new member
    emulNet->ENsend(&memberNode->addr, &newEntryAddr, (char *)msg, msgsize);
    
    free(msg);
    
    // add to your own member list vector
    memberNode->memberList.push_back(newEntry);
    hasUpdatesToGive = true;
    
#ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, &newEntryAddr);
#endif
}

void MP1Node::updateMembershipList(char *data) {
    hasUpdatesToGive = false;
    
    size_t memberListSize;
    memcpy(&memberListSize, data, sizeof(size_t));
    
    size_t memOffset = sizeof(size_t);
    
    for (size_t i = 0; i != memberListSize; ++i) {
        int id;
        short port;
        long heartbeat;
        
        memcpy(&id, data + memOffset, sizeof(int));
        memOffset += sizeof(int);
        
        memcpy(&port, data + memOffset, sizeof(short));
        memOffset += sizeof(short);
        
        memcpy(&heartbeat, data + memOffset, sizeof(long));
        memOffset += sizeof(long);
        
        MemberListEntry *member = getMemberFromMemberList(id);
        // update heartbeat for member node if it exists
        if (member && heartbeat > member->heartbeat) {
            member->setheartbeat(heartbeat);
            member->settimestamp(memberNode->timeOutCounter);
            hasUpdatesToGive = true;
        }
        // or add it if it doesn't
        else if (!member) {
            member = new MemberListEntry(id, port, heartbeat, memberNode->timeOutCounter);
            memberNode->memberList.push_back(*member);
            
#ifdef DEBUGLOG
            Address memberAddress = getMemberListEntryAddress(member);
            log->logNodeAdd(&memberNode->addr, &memberAddress);
#endif
            
            delete member;
            hasUpdatesToGive = true;
        }
    }
}

void MP1Node::processPullRequest(char *data) {
    if (!hasUpdatesToGive) {
        return;
    }
    
    int id;
    memcpy(&id, data, sizeof(int));
    
    short port;
    memcpy(&port, data + sizeof(int), sizeof(short));
    
    MessageHdr *msg = nullptr;
    size_t msgsize = createHealthyMembershipListMsg(&msg, HEARTBEAT);
    
    Address memberAddress = getAddressFromIDAndPort(id, port);
    emulNet->ENsend(&memberNode->addr, &memberAddress, (char *)msg, msgsize);
    
    free(msg);
}

void MP1Node::processPing(char *data) {
    int id;
    memcpy(&id, data, sizeof(int));
    
    short port;
    memcpy(&port, data + sizeof(int), sizeof(short));
    
    Address pingerAddress = getAddressFromIDAndPort(id, port);
    
    MemberListEntry *pinger = getMemberFromMemberList(id);
    if (!pinger) {
//        pinger = new MemberListEntry(id, port, memberNode->timeOutCounter, memberNode->timeOutCounter);
        memberNode->memberList.push_back(MemberListEntry(id, port, memberNode->timeOutCounter, memberNode->timeOutCounter));
        
#ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &pingerAddress);
#endif
    }
    
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = ACK;
    memcpy((char*)(msg+1), &memberNode->addr.addr, sizeof(int));
    
    emulNet->ENsend(&memberNode->addr, &pingerAddress, (char *)msg, msgsize);
    
    free(msg);
}

void MP1Node::processAck(char *data) {
    int id;
    memcpy(&id, data, sizeof(int));
    
    auto it = pingedNodes.find(id);
    if (it != pingedNodes.end()) {
        pingedNodes.erase(it);
    }
}

void MP1Node::processPingToSurrogate(char *data) {
    //    std::unordered_set<int> randomIndices;
    //    int maxRandIndices = std::min(numTargetMembers, static_cast<int>(memberNode->memberList.size()) - 1);
    //    while (static_cast<int>(randomIndices.size()) < maxRandIndices) {
    //        // randomly get the index of your gossip target
    //        int randIdx = getRandomInteger(0, memberNode->memberList.size() - 1);
    //        MemberListEntry randomNode = memberNode->memberList[randIdx];
    //        if (memberNode->memberList[randIdx].getid() != getSelfId() && pingedNodes.find(randomNode.getid()) == pingedNodes.end()) {
    //            randomIndices.insert(randIdx);
    //        }
    //    }
    
    int pingerID;
    memcpy(&pingerID, data, sizeof(int));
    size_t memOffset = sizeof(int);
    
    short pingerPort;
    memcpy(&pingerPort, data + memOffset, sizeof(short));
    memOffset += sizeof(short);
    
    int pingeeID;
    memcpy(&pingeeID, data + memOffset, sizeof(int));
    memOffset += sizeof(int);
    
    short pingeePort;
    memcpy(&pingeePort, data + sizeof(int), sizeof(short));
    
    Address pingerAddress = getAddressFromIDAndPort(pingerID, pingerPort);
    Address pingeeAddress = getAddressFromIDAndPort(pingeeID, pingeePort);
    
    // FIXME - duplicated code with MP1Node::processPing
    MemberListEntry *pinger = getMemberFromMemberList(pingerID);
    if (!pinger) {
//        *pinger = MemberListEntry(pingerID, pingerPort, memberNode->timeOutCounter, memberNode->timeOutCounter);
        memberNode->memberList.push_back(MemberListEntry(pingerID, pingerPort, memberNode->timeOutCounter, memberNode->timeOutCounter));
        
#ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &pingerAddress);
#endif
    }
    
    size_t msgsize = sizeof(MessageHdr) + 2 * sizeof(memberNode->addr.addr);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = PINGFROMSURROGATE;
    memcpy((char*)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char*)(msg+1), &pingerAddress.addr, sizeof(pingerAddress.addr));
    
    emulNet->ENsend(&memberNode->addr, &pingeeAddress, (char *)msg, msgsize);
    
    free(msg);
}

void MP1Node::processPingFromSurrogate(char *data) {
    // FIXME - code duplicated between this and MP1Node::processPingToSurrogate
    int surrogateID;
    memcpy(&surrogateID, data, sizeof(int));
    size_t memOffset = sizeof(int);
    
    short surrogatePort;
    memcpy(&surrogatePort, data + memOffset, sizeof(short));
    memOffset += sizeof(short);
    
    int origPingerID;
    memcpy(&origPingerID, data + memOffset, sizeof(int));
    memOffset += sizeof(int);
    
    short origPingerPort;
    memcpy(&origPingerPort, data + sizeof(int), sizeof(short));
    
    Address surrogateAddress = getAddressFromIDAndPort(surrogateID, surrogatePort);
    Address origPingerAddress = getAddressFromIDAndPort(origPingerID, origPingerPort);
    
    // FIXME - duplicated code with MP1Node::processPing
    MemberListEntry *pinger = getMemberFromMemberList(origPingerID);
    if (!pinger) {
//        *pinger = MemberListEntry(origPingerID, origPingerPort, memberNode->timeOutCounter, memberNode->timeOutCounter);
        memberNode->memberList.push_back(MemberListEntry(origPingerID, origPingerPort, memberNode->timeOutCounter, memberNode->timeOutCounter));
        
#ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &origPingerAddress);
#endif
    }
    
    // FIXME - duplicated code with MP1Node::processPing
    MemberListEntry *surrogate = getMemberFromMemberList(surrogateID);
    if (!surrogate) {
//        *surrogate = MemberListEntry(surrogateID, surrogatePort, memberNode->timeOutCounter, memberNode->timeOutCounter);
        memberNode->memberList.push_back(MemberListEntry(surrogateID, surrogatePort, memberNode->timeOutCounter, memberNode->timeOutCounter));
        
#ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &surrogateAddress);
#endif
    }
    
    size_t msgsize = sizeof(MessageHdr) + 2 * sizeof(memberNode->addr.addr);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = ACKTOSURROGATE;
    memcpy((char*)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char*)(msg+1), &origPingerAddress.addr, sizeof(origPingerAddress.addr));
    
    emulNet->ENsend(&memberNode->addr, &surrogateAddress, (char *)msg, msgsize);
    
    free(msg);
}

void MP1Node::processAckToSurrogate(char *data) {
    // FIXME - code duplicated between this and MP1Node::processPingToSurrogate
    int pingeeID;
    memcpy(&pingeeID, data, sizeof(int));
    size_t memOffset = sizeof(int);
    
    short pingeePort;
    memcpy(&pingeePort, data + memOffset, sizeof(short));
    memOffset += sizeof(short);
    
    int pingerID;
    memcpy(&pingerID, data + memOffset, sizeof(int));
    memOffset += sizeof(int);
    
    short pingerPort;
    memcpy(&pingerPort, data + sizeof(int), sizeof(short));
    
    Address pingeeAddress = getAddressFromIDAndPort(pingeeID, pingeePort);
    Address pingerAddress = getAddressFromIDAndPort(pingerID, pingerPort);
    
    // FIXME - duplicated code with MP1Node::processPing
    MemberListEntry *pingee = getMemberFromMemberList(pingeeID);
    if (!pingee) {
//        *pingee = MemberListEntry(pingeeID, pingeePort, memberNode->timeOutCounter, memberNode->timeOutCounter);
        memberNode->memberList.push_back(MemberListEntry(pingeeID, pingeePort, memberNode->timeOutCounter, memberNode->timeOutCounter));
        
#ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &pingeeAddress);
#endif
    }
    
    size_t msgsize = sizeof(MessageHdr) + sizeof(pingeeAddress.addr);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = ACKFROMSURROGATE;
    memcpy((char*)(msg+1), &pingeeAddress.addr, sizeof(pingeeAddress));
    
    emulNet->ENsend(&memberNode->addr, &pingerAddress, (char *)msg, msgsize);
    
    free(msg);
}

void MP1Node::processAckFromSurrogate(char *data) {
    // FIXME - this shouldn't be a method, just use MP1Node::processAck in the abvoe switch statement
    processAck(data);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    if (memberNode->pingCounter > 0) {
        memberNode->pingCounter--;
    }
    else {
        memberNode->pingCounter = TGOSSIP;
        memberNode->heartbeat++;
        
        switch (protocol) {
            case ALLTOALL:
                allToAllBroadcast();
                break;
            case PUSHGOSSIP:
                pushGossipBroadcast();
                break;
            case PULLGOSSIP:
                pullGossipBroadcast();
                break;
            case SWIM:
                pingRandomNode();
                break;
            default:
                break;
        }
    }
    
    
    
    
    for(auto it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
        auto mapIt = pingedNodes.find(it->getid());
        if (mapIt == pingedNodes.end()) {
            ++it;
            continue;
        }
        if(memberNode->timeOutCounter - mapIt->second > TREMOVE) {
            pingedNodes.erase(mapIt);
#ifdef DEBUGLOG
            Address memberAddress = getAddressFromIDAndPort(it->getid(), it->getport());
            log->logNodeRemove(&memberNode->addr, &memberAddress);
#endif
            it = memberNode->memberList.erase(it);
        }
        else if (memberNode->timeOutCounter - mapIt->second > TFAIL) {
            std::unordered_set<int> randomIndices;
            int maxRandIndices = std::min(numTargetMembers, static_cast<int>(memberNode->memberList.size()) - 1 - static_cast<int>(pingedNodes.size()));
            while (static_cast<int>(randomIndices.size()) < maxRandIndices) {
                // randomly get the index of your gossip target
                int randIdx = getRandomInteger(0, memberNode->memberList.size() - 1);
                MemberListEntry randomNode = memberNode->memberList[randIdx];
                if (memberNode->memberList[randIdx].getid() != getSelfId() && pingedNodes.find(randomNode.getid()) == pingedNodes.end()) {
                    randomIndices.insert(randIdx);
                }
            }
            
            
            size_t msgsize = sizeof(MessageHdr) + 2 * sizeof(memberNode->addr.addr);
            MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
            msg->msgType = PINGTOSURROGATE;
            memcpy((char*)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
            
            Address pingeeAddress = getAddressFromIDAndPort(it->getid(), it->getport());
            memcpy((char*)(msg+1), &pingeeAddress.addr, sizeof(pingeeAddress.addr));
            
            for (int idx : randomIndices) {
                MemberListEntry member = memberNode->memberList[idx];
                
                Address memberAddress = getMemberListEntryAddress(&member);
                emulNet->ENsend(&memberNode->addr, &memberAddress, (char *)msg, msgsize);
            }
            
            free(msg);
            
            ++it;
            continue;
            
            
        }
        else {
            ++it;
            continue;
        }
    }
    
    
    
    
    
    
    
//    removeFailedMembers();
    
    memberNode->timeOutCounter++;
    
    // Update myself in my own membership list so that I don't have to rely on others for info about myself
    // Plus now I can't delete myself from my own list if I haven't heard about myself in a while
    MemberListEntry *self = getMemberFromMemberList(getSelfId());
    if (self) {
        self->setheartbeat(memberNode->heartbeat);
        self->settimestamp(memberNode->timeOutCounter);
    }
}

void MP1Node::allToAllBroadcast() {
    size_t numMembersInMessage = 1; // needed for compatibility with push gossip heartbeat messages
    for (auto &memberListEntry : memberNode->memberList) {
        // create HEARTBEAT message: format of data is {struct MessageHdr + the number 1 + self data}
        size_t msgsize = sizeof(MessageHdr) + sizeof(size_t) + sizeof(memberNode->addr.addr) + sizeof(long);
        MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
        msg->msgType = HEARTBEAT;
        
        memcpy((char *)(msg+1), &numMembersInMessage, sizeof(size_t));
        size_t memOffset = sizeof(size_t);
        
        memcpy((char*)(msg+1) + memOffset, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memOffset += sizeof(memberNode->addr.addr);
        
        memcpy((char*)(msg+1) + memOffset, &memberNode->heartbeat, sizeof(long));
        
        // Send HEARTBEAT message to destination node
        Address memberAddress = getMemberListEntryAddress(&memberListEntry);
        emulNet->ENsend(&memberNode->addr, &memberAddress, (char *)msg, msgsize);
        
        free(msg);
    }
}

void MP1Node::pushGossipBroadcast() {
    std::unordered_set<int> randomIndices;
    int maxRandIndices = std::min(numTargetMembers, static_cast<int>(memberNode->memberList.size()) - 1);
    while (static_cast<int>(randomIndices.size()) < maxRandIndices) {
        // randomly get the index of your gossip target
        int randIdx = getRandomInteger(0, memberNode->memberList.size() - 1);
        if (memberNode->memberList[randIdx].getid() != getSelfId()) {
            randomIndices.insert(randIdx);
        }
    }
    
    MessageHdr *msg = nullptr;
    size_t msgsize = createHealthyMembershipListMsg(&msg, HEARTBEAT);
    
    for (int idx : randomIndices) {
        MemberListEntry member = memberNode->memberList[idx];
        
        Address memberAddress = getMemberListEntryAddress(&member);
        emulNet->ENsend(&memberNode->addr, &memberAddress, (char *)msg, msgsize);
    }
    
    free(msg);
}

void MP1Node::pullGossipBroadcast() {
    std::unordered_set<int> randomIndices;
    int maxRandIndices = std::min(numTargetMembers, static_cast<int>(memberNode->memberList.size()) - 1);
    while (static_cast<int>(randomIndices.size()) < maxRandIndices) {
        // randomly get the index of your gossip target
        int randIdx = getRandomInteger(0, memberNode->memberList.size() - 1);
        if (memberNode->memberList[randIdx].getid() != getSelfId()) {
            randomIndices.insert(randIdx);
        }
    }
    
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = PULLREQUEST;
    memcpy((char*)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    
    for (int idx : randomIndices) {
        MemberListEntry member = memberNode->memberList[idx];
        
        Address memberAddress = getMemberListEntryAddress(&member);
        emulNet->ENsend(&memberNode->addr, &memberAddress, (char *)msg, msgsize);
    }
    
    free(msg);
}

void MP1Node::pingRandomNode() {
    if (memberNode->memberList.size() < 2 || memberNode->memberList.size() - pingedNodes.size() < 2) {
        return;
    }
    
    int randIdx;
    MemberListEntry randomNode;
    do {
        randIdx = getRandomInteger(0, memberNode->memberList.size());
        randomNode = memberNode->memberList[randIdx];
    } while (randomNode.getid() == getSelfId() || pingedNodes.find(randomNode.getid()) != pingedNodes.end());
    
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = PING;
    memcpy((char*)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    
    Address randomAndress = getMemberListEntryAddress(&randomNode);
    emulNet->ENsend(&memberNode->addr, &randomAndress, (char *)msg, msgsize);
    
    pingedNodes[randomNode.getid()] = memberNode->timeOutCounter;
    
    free(msg);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;
    
    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;
    
    return joinaddr;
}

Address MP1Node::getMemberListEntryAddress(MemberListEntry *entry) {
    Address entryaddr;
    
    memset(&entryaddr, 0, sizeof(Address));
    *(int *)(&entryaddr.addr) = entry->getid();
    *(short *)(&entryaddr.addr[4]) = entry->getport();
    
    return entryaddr;
}

Address MP1Node::getAddressFromIDAndPort(int id, short port) {
    Address entryaddr;
    
    memset(&entryaddr, 0, sizeof(Address));
    *(int *)(&entryaddr.addr) = id;
    *(short *)(&entryaddr.addr[4]) = port;
    
    return entryaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
    
    MemberListEntry self(getSelfId(), getSelfPort(), memberNode->heartbeat, memberNode->timeOutCounter);
    memberNode->memberList.push_back(self);
    
#ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, &memberNode->addr);
#endif
}

size_t MP1Node::removeFailedMembers() {
    size_t membersRemoved = memberNode->memberList.size();
    
    for(auto it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
        if(memberNode->timeOutCounter - it->gettimestamp() > TREMOVE) {
#ifdef DEBUGLOG
            Address memberAddress = getAddressFromIDAndPort(it->getid(), it->getport());
            log->logNodeRemove(&memberNode->addr, &memberAddress);
#endif
            it = memberNode->memberList.erase(it);
        }
        else {
            ++it;
        }
    }
    
    membersRemoved -= memberNode->memberList.size();
    return membersRemoved;
}

size_t MP1Node::getNumberOfHealthyMembers() {
    size_t numHealthyMembers = 0;
    for (auto &memberListEntry: memberNode->memberList) {
        if (memberNode->timeOutCounter - memberListEntry.gettimestamp() > TFAIL) {
            continue;
        }
        ++numHealthyMembers;
    }
    return numHealthyMembers;
}

size_t MP1Node::createHealthyMembershipListMsg(MessageHdr **msg, MsgTypes msgType) {
    size_t numHealthyMembers = getNumberOfHealthyMembers();
    size_t msgsize = sizeof(MessageHdr) + sizeof(size_t) + numHealthyMembers * (sizeof(int) + sizeof(short) + sizeof(long));
    
    // create message: format of data is {struct MessageHdr + num of healthy members + the member list}
    *msg = (MessageHdr *) malloc(msgsize * sizeof(char)); // should be freed by caller
    (*msg)->msgType = msgType;
    memcpy((char *)(*msg+1), &numHealthyMembers, sizeof(size_t));
    
    size_t memOffset = sizeof(size_t);
    for (auto &memberListEntry: memberNode->memberList) {
        if (memberNode->timeOutCounter - memberListEntry.gettimestamp() > TFAIL) {
            continue;
        }
        
        memcpy((char *)(*msg+1) + memOffset, &memberListEntry.id, sizeof(int));
        memOffset += sizeof(int);
        
        memcpy((char *)(*msg+1) + memOffset, &memberListEntry.port, sizeof(short));
        memOffset += sizeof(short);
        
        memcpy((char *)(*msg+1) + memOffset, &memberListEntry.heartbeat, sizeof(long));
        memOffset += sizeof(long);
    }
    
    return msgsize;
}

MemberListEntry* MP1Node::getMemberFromMemberList(int id) {
    MemberListEntry *foundEntry = nullptr;
    
    for (auto &memberListEntry : memberNode->memberList) {
        if (memberListEntry.getid() == id) {
            foundEntry = &memberListEntry;
            break;
        }
    }
    
    return foundEntry;
}

/**
 * FUNCTION NAME: getRandomInteger
 *
 * DESCRIPTION: gets a random integer in the inclusive range [begin, end]
 * DISCLAIMER: this was copied from a cppreference.com code snippet with minor modifications
 * http://en.cppreference.com/w/cpp/numeric/random/uniform_int_distribution
 */
int MP1Node::getRandomInteger(int begin, int end) {
    static std::random_device rd;  // Will be used to obtain a seed for the random number engine
    static std::mt19937 gen(rd()); // Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(begin, end);
    // Use dis to transform the random unsigned int generated by gen into an int in [begin, end]
    return dis(gen);
}
