/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

// TODO - Refactor each protocol implementation into its own Node class that extends MP1Node

const FailureDetectionProtocol MP1Node::protocol = SWIM;

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, EmulNet *emul, Log *log, Address *address) {
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
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
    if ( this->memberNode->bFailed ) {
        return false;
    }
    else {
        return this->emulNet->ENrecv(&(this->memberNode->addr), enqueueWrapper, NULL, 1, &(this->memberNode->mp1q));
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
    joinaddr = getAddressFromIDAndPort(1, 0); // address of the coordinator
    
    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        this->log->LOG(&(this->memberNode->addr), "init_thisnode failed. Exit.");
#endif
        exit(1);
    }
    
    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        this->log->LOG(&(this->memberNode->addr), "Unable to join self to group. Exiting.");
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
    this->memberNode->bFailed = false;
    this->memberNode->inited = true;
    this->memberNode->inGroup = false;
    // node is up!
    this->memberNode->nnb = 0;
    this->memberNode->heartbeat = 0;
    this->memberNode->pingCounter = TGOSSIP;
    this->memberNode->timeOutCounter = -1;
    initMemberListTable();
    
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
    
    if ( 0 == memcmp((char *)&(this->memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(this->memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        this->log->LOG(&(this->memberNode->addr), "Starting up group...");
#endif
        this->memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &(this->memberNode->addr.addr), sizeof(this->memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(this->memberNode->addr.addr), &(this->memberNode->heartbeat), sizeof(long));
        
#ifdef DEBUGLOG
        this->log->LOG(&(this->memberNode->addr), "Trying to join...");
#endif
        
        // send JOINREQ message to introducer member
        this->emulNet->ENsend(&(this->memberNode->addr), joinaddr, (char *)msg, msgsize);
        
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
    this->memberNode->bFailed = true;
    this->memberNode->inited = false;
    this->memberNode->inGroup = false;
    this->memberNode->nnb = 0;
    this->memberNode->heartbeat = 0;
    this->memberNode->pingCounter = TGOSSIP;
    this->memberNode->timeOutCounter = -1;
    this->memberNode->memberList.clear();
    
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (this->memberNode->bFailed) {
        return;
    }
    
    // Check my messages
    checkMessages();
    
    // Wait until you're in the group...
    if( !this->memberNode->inGroup ) {
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
    while ( !this->memberNode->mp1q.empty() ) {
        ptr = this->memberNode->mp1q.front().elt;
        size = this->memberNode->mp1q.front().size;
        this->memberNode->mp1q.pop();
        recvCallBack((void *)this->memberNode, (char *)ptr, size);
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
            this->memberNode->inGroup = true;
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
        case ACKFROMSURROGATE:
            processAck(dataWithoutHeader);
            break;
        case PINGTOSURROGATE:
            processMessageToSurrogate(dataWithoutHeader, true);
            break;
        case PINGFROMSURROGATE:
            processPingFromSurrogate(dataWithoutHeader);
            break;
        case ACKTOSURROGATE:
            processMessageToSurrogate(dataWithoutHeader, false);
            break;
        default:
            messageProcessed = false;
            break;
    }
    
    return messageProcessed;
}

/**
 * FUNCTION NAME: processJoinRequest
 *
 * DESCRIPTION: Process request from node wanting to join the group
 */
void MP1Node::processJoinRequest(char *data) {
    // get member list entry info from data
    int id;
    short port;
    long heartbeat;
    
    memcpy(&id, data, sizeof(int));
    size_t offset = sizeof(int);
    
    memcpy(&port, data + offset, sizeof(short));
    offset += sizeof(short);
    
    memcpy(&heartbeat, data + offset, sizeof(long));
    
    MessageHdr *msg = nullptr;
    size_t msgsize = createHealthyMembershipListMsg(&msg, JOINREP);
    
    MemberListEntry newEntry(id, port, heartbeat, this->memberNode->timeOutCounter);
    
    Address newEntryAddr = getAddressFromIDAndPort(newEntry.getid(), newEntry.getport());
    
    // send JOINREP message to new member
    this->emulNet->ENsend(&(this->memberNode->addr), &newEntryAddr, (char *)msg, msgsize);
    
    free(msg);
    
    // add to your own member list and update buffer
    this->memberNode->memberList.emplace_back(newEntry);
    addNodeToBuffer(this->updatedNodeBuffer, newEntry);
    this->hasUpdatesToGive = true;
    
#ifdef DEBUGLOG
    this->log->logNodeAdd(&(this->memberNode->addr), &newEntryAddr);
#endif
}

/**
 * FUNCTION NAME: updateMembershipList
 *
 * DESCRIPTION: Update your membership list based on the information sent to you
 */
void MP1Node::updateMembershipList(char *data) {
    this->hasUpdatesToGive = false;
    
    size_t memberListSize;
    memcpy(&memberListSize, data, sizeof(size_t));
    
    size_t offset = sizeof(size_t);
    
    for (size_t i = 0; i != memberListSize; ++i) {
        int id;
        short port;
        long heartbeat;
        
        memcpy(&id, data + offset, sizeof(int));
        offset += sizeof(int);
        
        memcpy(&port, data + offset, sizeof(short));
        offset += sizeof(short);
        
        memcpy(&heartbeat, data + offset, sizeof(long));
        offset += sizeof(long);
        
        updateNodeInMembershipList(id, port, heartbeat);
    }
}

/**
 * FUNCTION NAME: processPullRequest
 *
 * DESCRIPTION: Send your membership list to the node that pings you for it
 *              Used in the pull gossip protocol
 */
void MP1Node::processPullRequest(char *data) {
    if (!this->hasUpdatesToGive) {
        return;
    }
    
    int id;
    memcpy(&id, data, sizeof(int));
    
    short port;
    memcpy(&port, data + sizeof(int), sizeof(short));
    
    MessageHdr *msg = nullptr;
    size_t msgsize = createHealthyMembershipListMsg(&msg, HEARTBEAT);
    
    Address memberAddress = getAddressFromIDAndPort(id, port);
    this->emulNet->ENsend(&(this->memberNode->addr), &memberAddress, (char *)msg, msgsize);
    
    free(msg);
}

/**
 * FUNCTION NAME: processPing
 *
 * DESCRIPTION: Process membership updates and send an ack back to the node pinging you
 *              Used in the SWIM protocol
 */
void MP1Node::processPing(char *data) {
    int id;
    memcpy(&id, data, sizeof(int));
    size_t offset = sizeof(int);
    
    short port;
    memcpy(&port, data + offset, sizeof(short));
    offset += sizeof(short);
    
    Address pingerAddress = getAddressFromIDAndPort(id, port);
    
    // add the pinger to your membership list if it isn't there already
    ensureNodePresenceInList(id, port);

    // send an ack back to the pinger with your own membership updates
    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + 2 * sizeof(size_t) + (this->updatedNodeBuffer.size() + this->failedNodeBuffer.size()) * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = ACK;
    
    int selfId = getSelfId();
    memcpy((char*)(msg+1), &selfId, sizeof(int));
    
    addBufferInfoToMessage(&msg, sizeof(int));
    
    this->emulNet->ENsend(&(this->memberNode->addr), &pingerAddress, (char *)msg, msgsize);
    
    free(msg);
    
    incrementBufferCounts(this->updatedNodeBuffer);
    incrementBufferCounts(this->failedNodeBuffer);
    
    // finally read the updates the pinger had sent
    readBufferInfoFromMessage(data, offset);
}

/**
 * FUNCTION NAME: processAck
 *
 * DESCRIPTION: Process the ack and membership updates sent from a pinged node
 *              Used in the SWIM protocol
 */
void MP1Node::processAck(char *data) {
    int id;
    memcpy(&id, data, sizeof(int));
    
    auto it = this->pingedNodes.find(id);
    if (it != this->pingedNodes.end()) {
        this->pingedNodes.erase(it);
    }
    
    readBufferInfoFromMessage(data, sizeof(int));
}

/**
 * FUNCTION NAME: processMessageToSurrogate
 *
 * DESCRIPTION: Process pings and acks sent to the surrogate pinger
 *              Used in the SWIM protocol
 */
void MP1Node::processMessageToSurrogate(char *data, bool fromPinger) {
    int pingerID;
    memcpy(&pingerID, data, sizeof(int));
    size_t offset = sizeof(int);
    
    short pingerPort;
    memcpy(&pingerPort, data + offset, sizeof(short));
    offset += sizeof(short);
    
    int pingeeID;
    memcpy(&pingeeID, data + offset, sizeof(int));
    offset += sizeof(int);
    
    short pingeePort;
    memcpy(&pingeePort, data + sizeof(int), sizeof(short));
    offset += sizeof(short);
    
    Address pingerAddress = getAddressFromIDAndPort(pingerID, pingerPort);
    Address pingeeAddress = getAddressFromIDAndPort(pingeeID, pingeePort);
    
    // add the message sender to your membership list if it isn't there already
    int senderID = fromPinger ? pingerID : pingeeID;
    short senderPort = fromPinger ? pingerPort : pingeePort;
    ensureNodePresenceInList(senderID, senderPort);
    
    // prepare and pass the passed in message to the appropriate recipient (pinger or pingee)
    size_t joinedBufferSize;
    memcpy(&joinedBufferSize, data + offset, sizeof(size_t));
    offset += sizeof(size_t) + joinedBufferSize * sizeof(MemberListEntry);
    
    size_t failedBufferSize;
    memcpy(&failedBufferSize, data + offset, sizeof(size_t));

    if (fromPinger) {
        size_t msgsize = sizeof(MessageHdr) + 2 * sizeof(this->memberNode->addr.addr) + 2 * sizeof(size_t) + (joinedBufferSize + failedBufferSize) * sizeof(MemberListEntry);
        MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
        msg->msgType = PINGFROMSURROGATE;
        memcpy((char*)(msg+1), &(this->memberNode->addr.addr), sizeof(this->memberNode->addr.addr));
        offset = sizeof(this->memberNode->addr.addr);
        
        memcpy((char*)(msg+1) + offset, &pingerAddress.addr, sizeof(pingerAddress.addr));
        offset += sizeof(pingerAddress.addr);
        
        memcpy((char*)(msg+1) + offset, data + 2 * sizeof(int) + 2 * sizeof(short), 2 * sizeof(size_t) + (joinedBufferSize + failedBufferSize) * sizeof(MemberListEntry));
        
        this->emulNet->ENsend(&(this->memberNode->addr), &pingeeAddress, (char *)msg, msgsize);
        
        free(msg);
    } else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(int) + 2 * sizeof(size_t) + (joinedBufferSize + failedBufferSize) * sizeof(MemberListEntry);
        MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
        msg->msgType = ACKFROMSURROGATE;
        memcpy((char*)(msg+1), &pingeeID, sizeof(int));
        
        memcpy((char*)(msg+1) + sizeof(int), data + 2 * sizeof(int) + 2 * sizeof(short), 2 * sizeof(size_t) + (joinedBufferSize + failedBufferSize) * sizeof(MemberListEntry));
        
        this->emulNet->ENsend(&(this->memberNode->addr), &pingerAddress, (char *)msg, msgsize);
        
        free(msg);
    }
}

/**
 * FUNCTION NAME: processPingFromSurrogate
 *
 * DESCRIPTION: Process membership updates and send an ack back to the node pinging you on behalf of the original pinger
 *              Used in the SWIM protocol
 */
void MP1Node::processPingFromSurrogate(char *data) {
    int surrogateID;
    memcpy(&surrogateID, data, sizeof(int));
    size_t offset = sizeof(int);
    
    short surrogatePort;
    memcpy(&surrogatePort, data + offset, sizeof(short));
    offset += sizeof(short);
    
    int origPingerID;
    memcpy(&origPingerID, data + offset, sizeof(int));
    offset += sizeof(int);
    
    short origPingerPort;
    memcpy(&origPingerPort, data + sizeof(int), sizeof(short));
    offset += sizeof(short);
    
    Address surrogateAddress = getAddressFromIDAndPort(surrogateID, surrogatePort);
    Address origPingerAddress = getAddressFromIDAndPort(origPingerID, origPingerPort);
    
    // add the surrogate pinger to your membership list if it isn't there already
    ensureNodePresenceInList(surrogateID, surrogatePort);
    
    // send an ack back to the surrogate pinger with your own membership updates
    MessageHdr *msg = nullptr;
    size_t msgsize = prepareMessageForSurrogate(&msg, ACKTOSURROGATE, origPingerAddress, this->memberNode->addr);

    this->emulNet->ENsend(&(this->memberNode->addr), &surrogateAddress, (char *)msg, msgsize);
    
    free(msg);
    
    incrementBufferCounts(this->updatedNodeBuffer);
    incrementBufferCounts(this->failedNodeBuffer);
    
    // finally read the updates the surrogate pinger had sent
    readBufferInfoFromMessage(data, offset);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    if (this->memberNode->pingCounter > 0) {
        this->memberNode->pingCounter--;
    }
    else {
        this->memberNode->pingCounter = TGOSSIP;
        this->memberNode->heartbeat++;
        
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
    
    if (MP1Node::protocol == SWIM) {
        for(auto it = this->memberNode->memberList.begin(); it != this->memberNode->memberList.end();) {
            auto mapIt = this->pingedNodes.find(it->getid());
            if (mapIt == this->pingedNodes.end()) {
                ++it;
                continue;
            }
            // pinged node has failed, remove it
            if(this->memberNode->timeOutCounter - mapIt->second > TREMOVE) {
                this->pingedNodes.erase(mapIt);
#ifdef DEBUGLOG
                Address memberAddress = getAddressFromIDAndPort(it->getid(), it->getport());
                this->log->logNodeRemove(&(this->memberNode->addr), &memberAddress);
#endif
                addNodeToBuffer(this->failedNodeBuffer, *it);
                it = this->memberNode->memberList.erase(it);
            }
            // pinged node is taking too long to reply. try to reach it via another node
            else if (this->memberNode->timeOutCounter - mapIt->second > TFAIL) {
                std::unordered_set<int> randomIndices;
                int maxRandIndices = std::min(TARGETMEMBERS, static_cast<int>(this->memberNode->memberList.size()) - 1 - static_cast<int>(this->pingedNodes.size()));
                while (static_cast<int>(randomIndices.size()) < maxRandIndices) {
                    // randomly get the index of your gossip target
                    int randIdx = getRandomInteger(0, this->memberNode->memberList.size() - 1);
                    MemberListEntry randomNode = this->memberNode->memberList[randIdx];
                    if (this->memberNode->memberList[randIdx].getid() != getSelfId() && this->pingedNodes.find(randomNode.getid()) == this->pingedNodes.end()) {
                        randomIndices.insert(randIdx);
                    }
                }
                
                // prepare and send a new ping message to random targets for them to forward
                MessageHdr *msg = nullptr;
                Address pingeeAddress = getAddressFromIDAndPort(it->getid(), it->getport());
                size_t msgsize = prepareMessageForSurrogate(&msg, PINGTOSURROGATE, this->memberNode->addr, pingeeAddress);
                
                for (int idx : randomIndices) {
                    MemberListEntry member = this->memberNode->memberList[idx];
                    
                    Address memberAddress = getAddressFromIDAndPort(member.getid(), member.getport());
                    this->emulNet->ENsend(&(this->memberNode->addr), &memberAddress, (char *)msg, msgsize);
                }
                
                free(msg);
                ++it;
            }
            else {
                ++it;
            }
        }
        cleanBuffer(this->updatedNodeBuffer);
        cleanBuffer(this->failedNodeBuffer);
    } else {
        removeFailedMembers();
    }
    
    this->memberNode->timeOutCounter++;
    
    // update myself in my own membership list
    MemberListEntry *self = getMemberFromMemberList(getSelfId());
    if (self) {
        self->setheartbeat(this->memberNode->heartbeat);
        self->settimestamp(this->memberNode->timeOutCounter);
    }
}

/**
 * FUNCTION NAME: allToAllBroadcast
 *
 * DESCRIPTION: Broadcast a heartbeat message to all other nodes in your membership list
 */
void MP1Node::allToAllBroadcast() {
    size_t numMembersInMessage = 1; // needed for compatibility with push gossip heartbeat messages
    for (auto &memberListEntry : this->memberNode->memberList) {
        // create HEARTBEAT message: format of data is {struct MessageHdr + the number 1 + self data}
        size_t msgsize = sizeof(MessageHdr) + sizeof(size_t) + sizeof(this->memberNode->addr.addr) + sizeof(long);
        MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
        msg->msgType = HEARTBEAT;
        
        memcpy((char *)(msg+1), &numMembersInMessage, sizeof(size_t));
        size_t offset = sizeof(size_t);
        
        memcpy((char*)(msg+1) + offset, &(this->memberNode->addr.addr), sizeof(this->memberNode->addr.addr));
        offset += sizeof(this->memberNode->addr.addr);
        
        memcpy((char*)(msg+1) + offset, &(this->memberNode->heartbeat), sizeof(long));
        
        // Send HEARTBEAT message to destination node
        Address memberAddress = getAddressFromIDAndPort(memberListEntry.getid(), memberListEntry.getport());
        this->emulNet->ENsend(&(this->memberNode->addr), &memberAddress, (char *)msg, msgsize);
        
        free(msg);
    }
}

/**
 * FUNCTION NAME: pushGossipBroadcast
 *
 * DESCRIPTION: Push a heartbeat message with your membership list info to random nodes
 */
void MP1Node::pushGossipBroadcast() {
    std::unordered_set<int> randomIndices;
    int maxRandIndices = std::min(TARGETMEMBERS, static_cast<int>(this->memberNode->memberList.size()) - 1);
    while (static_cast<int>(randomIndices.size()) < maxRandIndices) {
        // randomly get the index of your gossip target
        int randIdx = getRandomInteger(0, this->memberNode->memberList.size() - 1);
        if (this->memberNode->memberList[randIdx].getid() != getSelfId()) {
            randomIndices.insert(randIdx);
        }
    }
    
    MessageHdr *msg = nullptr;
    size_t msgsize = createHealthyMembershipListMsg(&msg, HEARTBEAT);
    
    for (int idx : randomIndices) {
        MemberListEntry member = this->memberNode->memberList[idx];
        
        Address memberAddress = getAddressFromIDAndPort(member.getid(), member.getport());
        this->emulNet->ENsend(&(this->memberNode->addr), &memberAddress, (char *)msg, msgsize);
    }
    
    free(msg);
}

/**
 * FUNCTION NAME: pullGossipBroadcast
 *
 * DESCRIPTION: Send a heartbeat message requesting membership list info to random nodes
 */
void MP1Node::pullGossipBroadcast() {
    std::unordered_set<int> randomIndices;
    int maxRandIndices = std::min(TARGETMEMBERS, static_cast<int>(this->memberNode->memberList.size()) - 1);
    while (static_cast<int>(randomIndices.size()) < maxRandIndices) {
        // randomly get the index of your gossip target
        int randIdx = getRandomInteger(0, this->memberNode->memberList.size() - 1);
        if (this->memberNode->memberList[randIdx].getid() != getSelfId()) {
            randomIndices.insert(randIdx);
        }
    }
    
    size_t msgsize = sizeof(MessageHdr) + sizeof(this->memberNode->addr.addr);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = PULLREQUEST;
    memcpy((char*)(msg+1), &(this->memberNode->addr.addr), sizeof(this->memberNode->addr.addr));
    
    for (int idx : randomIndices) {
        MemberListEntry member = this->memberNode->memberList[idx];
        
        Address memberAddress = getAddressFromIDAndPort(member.getid(), member.getport());
        this->emulNet->ENsend(&(this->memberNode->addr), &memberAddress, (char *)msg, msgsize);
    }
    
    free(msg);
}

/**
 * FUNCTION NAME: pingRandomNode
 *
 * DESCRIPTION: Ping a random node to check its health and to send your memberhsip updates to
 *              Used in the SWIM protocol
 */
void MP1Node::pingRandomNode() {
    if (this->memberNode->memberList.size() < 2 || this->memberNode->memberList.size() - this->pingedNodes.size() < 2) {
        return;
    }
    
    // select the node randomly
    int randIdx;
    MemberListEntry randomNode;
    do {
        randIdx = getRandomInteger(0, this->memberNode->memberList.size());
        randomNode = this->memberNode->memberList[randIdx];
    } while (randomNode.getid() == getSelfId() || this->pingedNodes.find(randomNode.getid()) != this->pingedNodes.end());
    
    // then ping it
    size_t msgsize = sizeof(MessageHdr) + sizeof(this->memberNode->addr.addr) + 2 * sizeof(size_t) + (this->updatedNodeBuffer.size() + this->failedNodeBuffer.size()) * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = PING;
    memcpy((char*)(msg+1), &(this->memberNode->addr.addr), sizeof(this->memberNode->addr.addr));
    
    addBufferInfoToMessage(&msg, sizeof(this->memberNode->addr.addr));
    
    Address randomAndress = getAddressFromIDAndPort(randomNode.getid(), randomNode.getport());
    this->emulNet->ENsend(&(this->memberNode->addr), &randomAndress, (char *)msg, msgsize);
    
    this->pingedNodes[randomNode.getid()] = this->memberNode->timeOutCounter;
    incrementBufferCounts(this->updatedNodeBuffer);
    incrementBufferCounts(this->failedNodeBuffer);
    
    free(msg);
}

/**
 * FUNCTION NAME: prepareMessageForSurrogate
 *
 * DESCRIPTION: Construct a message to send to the surrogate pinger
 */
size_t MP1Node::prepareMessageForSurrogate(MessageHdr **msg, MsgTypes msgType, Address& pinger, Address& pingee) {
    size_t msgsize = sizeof(MessageHdr) + 2 * sizeof(this->memberNode->addr.addr) + 2 * sizeof(size_t) + (this->updatedNodeBuffer.size() + this->failedNodeBuffer.size()) * sizeof(MemberListEntry);
    *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    (*msg)->msgType = msgType;
    memcpy((char*)(*msg+1), pinger.addr, sizeof(pinger.addr));
    size_t offset = sizeof(pinger.addr);
    
    memcpy((char*)(*msg+1) + offset, &pingee.addr, sizeof(pingee.addr));
    offset += sizeof(pingee.addr);
    
    addBufferInfoToMessage(msg, offset);
    
    return msgsize;
}

/**
 * FUNCTION NAME: cleanBuffer
 *
 * DESCRIPTION: Remove all stale membership updates from the parameter buffer
 */
void MP1Node::cleanBuffer(std::vector<MembershipUpdate>& buffer) {
    for (auto it = buffer.begin(); it != buffer.end();) {
        if (it->count >= BUFFERTHRESHOLD) {
            it = buffer.erase(it);
        } else {
            ++it;
        }
    }
}

/**
 * FUNCTION NAME: incrementBufferCounts
 *
 * DESCRIPTION: Increase the ping count for all membership updates in the parameter buffer
 */
void MP1Node::incrementBufferCounts(std::vector<MembershipUpdate>& buffer) {
    for (MembershipUpdate& update : buffer) {
        update.count++;
    }
}

/**
 * FUNCTION NAME: addBufferInfoToMessage
 *
 * DESCRIPTION: Place membership updates in updated and failed buffers into parameter message
 */
void MP1Node::addBufferInfoToMessage(MessageHdr **msg, size_t offset) {
    size_t bufferSize = this->updatedNodeBuffer.size();
    memcpy((char*)(*msg+1) + offset, &bufferSize, sizeof(size_t));
    
    offset += sizeof(size_t);
    for (MembershipUpdate& update : this->updatedNodeBuffer) {
        memcpy((char*)(*msg+1) + offset, &update.node, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);
    }
    
    bufferSize = this->failedNodeBuffer.size();
    memcpy((char*)(*msg+1) + offset, &bufferSize, sizeof(size_t));
    
    offset += sizeof(size_t);
    for (MembershipUpdate& update : this->failedNodeBuffer) {
        memcpy((char*)(*msg+1) + offset, &update.node, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);
    }
}

/**
 * FUNCTION NAME: readBufferInfoFromMessage
 *
 * DESCRIPTION: Read membership updates and use them to update your own membership list
 */
void MP1Node::readBufferInfoFromMessage(char *data, size_t offset) {
    size_t bufferSize;
    memcpy(&bufferSize, data + offset, sizeof(size_t));
    offset += sizeof(size_t);
    
    for (size_t i = 0; i != bufferSize; ++i) {
        MemberListEntry node;
        memcpy(&node, data + offset, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);
        
        updateNodeInMembershipList(node.getid(), node.getport(), node.getheartbeat());
    }
    
    memcpy(&bufferSize, data + offset, sizeof(size_t));
    offset += sizeof(size_t);
    
    for (size_t i = 0; i != bufferSize; ++i) {
        MemberListEntry node;
        memcpy(&node, data + offset, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);
        
        for (auto it = this->memberNode->memberList.begin(); it != this->memberNode->memberList.end();) {
            if (it->id == node.getid() && node.getheartbeat() > it->heartbeat) {
#ifdef DEBUGLOG
                Address memberAddress = getAddressFromIDAndPort(it->getid(), it->getport());
                this->log->logNodeRemove(&(this->memberNode->addr), &memberAddress);
#endif
                addNodeToBuffer(this->failedNodeBuffer, *it);
                
                it = this->memberNode->memberList.erase(it);
            } else {
                ++it;
            }
        }
    }
}

/**
 * FUNCTION NAME: getAddressFromIDAndPort
 *
 * DESCRIPTION: Returns an address composed from the parameter ID and port
 */
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
void MP1Node::initMemberListTable() {
    this->memberNode->memberList.clear();
    
    MemberListEntry self(getSelfId(), getSelfPort(), this->memberNode->heartbeat, this->memberNode->timeOutCounter);
    this->memberNode->memberList.emplace_back(self);
    
#ifdef DEBUGLOG
    this->log->logNodeAdd(&(this->memberNode->addr), &(this->memberNode->addr));
#endif
}

/**
 * FUNCTION NAME: removeFailedMembers
 *
 * DESCRIPTION: Remove nodes from your membership list that have not replied to heartbeats
 */
void MP1Node::removeFailedMembers() {
    for(auto it = this->memberNode->memberList.begin(); it != this->memberNode->memberList.end();) {
        if(this->memberNode->timeOutCounter - it->gettimestamp() > TREMOVE) {
#ifdef DEBUGLOG
            Address memberAddress = getAddressFromIDAndPort(it->getid(), it->getport());
            this->log->logNodeRemove(&(this->memberNode->addr), &memberAddress);
#endif
            it = this->memberNode->memberList.erase(it);
        }
        else {
            ++it;
        }
    }
}

/**
 * FUNCTION NAME: getNumberOfHealthyMembers
 *
 * DESCRIPTION: Returns number of nodes from your membership list that have successfully replied to recent heartbeats
 */
size_t MP1Node::getNumberOfHealthyMembers() {
    size_t numHealthyMembers = 0;
    for (auto &memberListEntry: this->memberNode->memberList) {
        if (this->memberNode->timeOutCounter - memberListEntry.gettimestamp() > TFAIL) {
            continue;
        }
        ++numHealthyMembers;
    }
    return numHealthyMembers;
}

/**
 * FUNCTION NAME: createHealthyMembershipListMsg
 *
 * DESCRIPTION: Construct a message containing only the healthy nodes of your membership list
 */
size_t MP1Node::createHealthyMembershipListMsg(MessageHdr **msg, MsgTypes msgType) {
    size_t numHealthyMembers = getNumberOfHealthyMembers();
    size_t msgsize = sizeof(MessageHdr) + sizeof(size_t) + numHealthyMembers * (sizeof(int) + sizeof(short) + sizeof(long));
    
    // create message: format of data is {struct MessageHdr + num of healthy members + the member list}
    *msg = (MessageHdr *) malloc(msgsize * sizeof(char)); // should be freed by caller
    (*msg)->msgType = msgType;
    memcpy((char *)(*msg+1), &numHealthyMembers, sizeof(size_t));
    
    size_t offset = sizeof(size_t);
    for (auto &memberListEntry: this->memberNode->memberList) {
        if (this->memberNode->timeOutCounter - memberListEntry.gettimestamp() > TFAIL) {
            continue;
        }
        
        memcpy((char *)(*msg+1) + offset, &memberListEntry.id, sizeof(int));
        offset += sizeof(int);
        
        memcpy((char *)(*msg+1) + offset, &memberListEntry.port, sizeof(short));
        offset += sizeof(short);
        
        memcpy((char *)(*msg+1) + offset, &memberListEntry.heartbeat, sizeof(long));
        offset += sizeof(long);
    }
    
    return msgsize;
}

/**
 * FUNCTION NAME: getMemberFromMemberList
 *
 * DESCRIPTION: Retrieve the node associated with the parameter ID
 */
MemberListEntry* MP1Node::getMemberFromMemberList(int id) {
    MemberListEntry *foundEntry = nullptr;
    
    for (auto &memberListEntry : this->memberNode->memberList) {
        if (memberListEntry.getid() == id) {
            foundEntry = &memberListEntry;
            break;
        }
    }
    
    return foundEntry;
}

/**
 * FUNCTION NAME: ensureNodePresenceInList
 *
 * DESCRIPTION: Checks if the node with the parameter properties exists in the membership list and adds it if not
 */
void MP1Node::ensureNodePresenceInList(int id, short port) {
    MemberListEntry *node = getMemberFromMemberList(id);
    if (!node) {
        this->memberNode->memberList.emplace_back(MemberListEntry(id, port, this->memberNode->timeOutCounter, this->memberNode->timeOutCounter));
        
#ifdef DEBUGLOG
        Address address = getAddressFromIDAndPort(id, port);
        this->log->logNodeAdd(&(this->memberNode->addr), &address);
#endif
    }
}

/**
 * FUNCTION NAME: addNodeToBuffer
 *
 * DESCRIPTION: Adds the parameter node's info to the parameter buffer, replacing existing stale info
 */
void MP1Node::addNodeToBuffer(std::vector<MembershipUpdate>& buffer, MemberListEntry& node) {
    for (auto it = buffer.begin(); it != buffer.end();) {
        if (it->node.getid() == node.getid()) {
            it = buffer.erase(it);
            break;
        } else {
            ++it;
        }
    }
    
    MembershipUpdate update;
    update.node = node;
    update.count = 0;
    buffer.emplace_back(update);
}

/**
 * FUNCTION NAME: updateNodeInMembershipList
 *
 * DESCRIPTION: Updates the node with the parameter properties in the membership list if it exists or adds it otherwise
 */
void MP1Node::updateNodeInMembershipList(int nodeId, short port, long heartbeat) {
    bool membershipListUpdated = false;
    MemberListEntry *member = getMemberFromMemberList(nodeId);
    // update heartbeat for member node if it exists
    if (member && heartbeat > member->heartbeat) {
        member->setheartbeat(heartbeat);
        member->settimestamp(this->memberNode->timeOutCounter);
        membershipListUpdated = true;
    }
    // or add it if it doesn't
    else if (!member) {
        member = new MemberListEntry(nodeId, port, heartbeat, this->memberNode->timeOutCounter);
        this->memberNode->memberList.emplace_back(*member);
        
#ifdef DEBUGLOG
        Address memberAddress = getAddressFromIDAndPort(member->getid(), member->getport());
        this->log->logNodeAdd(&(this->memberNode->addr), &memberAddress);
#endif
        
        delete member;
        membershipListUpdated = true;
    }
    
    if (membershipListUpdated) {
        addNodeToBuffer(this->updatedNodeBuffer, *member);
        this->hasUpdatesToGive = true;
    }
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
