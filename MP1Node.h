/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 50
#define TFAIL 20
#define TGOSSIP 5
#define TARGETMEMBERS 2
#define BUFFERTHRESHOLD 10

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    HEARTBEAT,
    PULLREQUEST,
    
    // SWIM message types
    PING,
    ACK,
    PINGTOSURROGATE,
    PINGFROMSURROGATE,
    ACKTOSURROGATE,
    ACKFROMSURROGATE
};

/**
 * Failure Detection Protocol type
 */
enum FailureDetectionProtocol {
    ALLTOALL,
    PUSHGOSSIP,
    PULLGOSSIP,
    SWIM
};

/**
 * STRUCT NAME: MembershipUpdate
 *
 * DESCRIPTION: Keeps track of how many times information about a node was disseminated
 */
struct MembershipUpdate {
    size_t count;
    MemberListEntry node;
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
    enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
public:
    MP1Node(Member*, EmulNet*, Log*, Address*);
    void nodeStart(char *servaddrstr, short serverport);
    void recvLoop();
    void nodeLoop();
    void finishUpThisNode();
    inline Member* getMemberNode() {
        return memberNode;
    }
    virtual ~MP1Node();
    
private:
    EmulNet *emulNet;
    Log *log;
    Member *memberNode;
    
    // Set to true when membership list is updated, false otherwise
    bool hasUpdatesToGive;
    
    // Map to keep track of pinged nodes when using SWIM protocol
    std::unordered_map<int, int> pingedNodes;
    
    // Buffers to keep track of membership updates and how many times they have been disseminated
    std::vector<MembershipUpdate> updatedNodeBuffer;
    std::vector<MembershipUpdate> failedNodeBuffer;
    
    inline int getSelfId() {
        return *(int *)(&memberNode->addr.addr);
    }
    
    inline short getSelfPort() {
        return *(short *)(&memberNode->addr.addr[4]);
    }
    
    // The type of failure detection protocol to use
    static const FailureDetectionProtocol protocol;
    
    // Methods for managing node lifecycle and high level function
    int initThisNode(Address *joinaddr);
    void initMemberListTable();
    int introduceSelfToGroup(Address *joinAddress);
    static int enqueueWrapper(void *env, char *buff, int size);
    void checkMessages();
    bool recvCallBack(void *env, char *data, int size);
    void nodeLoopOps();
    
    // Methods for processing messages common to all protocols
    void processJoinRequest(char *data);
    void updateMembershipList(char *data);
    // Methods for sending and processing SWIM protocol messages
    void pingRandomNode();
    void processPing(char *data);
    void processAck(char *data);
    size_t prepareMessageForSurrogate(MessageHdr **msg, MsgTypes msgType, Address& pinger, Address& pingee);
    void processMessageToSurrogate(char *data, bool fromPinger);
    void processPingFromSurrogate(char *data);
    // Methods for sending and processing heartbeat-sending protocol messages
    void allToAllBroadcast();
    void pushGossipBroadcast();
    void pullGossipBroadcast();
    void processPullRequest(char *data);
    
    // Methods for managing and processing membership update buffers
    void addNodeToBuffer(std::vector<MembershipUpdate>& buffer, MemberListEntry& node);
    void addBufferInfoToMessage(MessageHdr **msg, size_t offset);
    void incrementBufferCounts(std::vector<MembershipUpdate>& buffer);
    void cleanBuffer(std::vector<MembershipUpdate>& buffer);
    void readBufferInfoFromMessage(char *data, size_t offset);
    
    // Methods for managing the membership list
    void ensureNodePresenceInList(int id, short port);
    void updateNodeInMembershipList(int nodeId, short port, long heartbeat);
    size_t getNumberOfHealthyMembers();
    size_t createHealthyMembershipListMsg(MessageHdr **msg, MsgTypes msgType);
    void removeFailedMembers();
    
    // ID and Address methods
    MemberListEntry* getMemberFromMemberList(int id);
    Address getAddressFromIDAndPort(int id, short port);
    
    // Utility method
    // FIXME - this method probably doesn't belong here
    int getRandomInteger(int begin, int end);
};

#endif /* _MP1NODE_H_ */
